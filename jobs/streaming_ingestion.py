"""JOB 1 / 3 — streaming ingestion: Kafka -> Iceberg (raw + quarantine).

Flow of one micro-batch (default every 10s):

    kafka topic `raw_transactions`
      -> from_json(EXPLICIT schema)         # no inferSchema, no surprises
      -> quality rules                       # bad rows -> raw.load_failures
      -> dedup inside the batch              # Kafka is at-least-once
      -> MERGE INTO raw.transactions_raw     # idempotent: replay-safe
      -> append raw.transactions_enriched    # the feature job reads this stream

Note what this job deliberately does NOT do: joins against slow dimension
tables.  Keeping the ingestion query dumb and fast is what protects your Kafka
lag when the enrichment layer has a bad day (see docs/04-streaming.md).

Usage
-----
    ./jobs/submit/run_job.sh streaming_ingestion                # inside compose
    python jobs/streaming_ingestion.py --starting earliest       # local, debug
    python jobs/streaming_ingestion.py --print-config            # no spark needed
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import cli, config, enrichment, schema, sparkutils, table_props  # noqa: E402

JOB = "streaming_ingestion"


def build_raw_table(spark) -> None:
    """Create the physical tables once (empty), so a first run never races DDL."""
    spark.sparkContext.setJobGroup("ddl", "create raw tables")
    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.TABLE_RAW} (
  transaction_id string,
  event_ts       string,
  event_ts_ts    timestamp,
  dt             date,
  card_id        string,
  merchant_id    string,
  amount         double,
  currency       string,
  channel        string,
  merchant_country string,
  card_present   boolean,
  merchant_category string,
  is_3ds         boolean,
  device_id      string,
  raw_json       string,
  dedup_key      string,
  source_topic   string,
  source_partition int,
  source_offset  bigint,
  kafka_ts       timestamp,
  ingest_ts      timestamp
) USING iceberg
PARTITIONED BY (dt)
TBLPROPERTIES (
  'write.format.default'='parquet',
  'write.distribution-mode'='hash',
  'write.metadata.delete-after-commit.enabled'='true'
)
""")
    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.TABLE_LOAD_FAILURES} (
  transaction_id string,
  payload_json   string,
  failure_reasons array<string>,
  source_topic   string,
  source_partition int,
  source_offset  bigint,
  kafka_ts       timestamp,
  ingest_ts      timestamp,
  dt             date
) USING iceberg
PARTITIONED BY (dt)
TBLPROPERTIES ('write.format.default'='parquet')
""")
    cols_ddl = ",\n  ".join(_enriched_ddl())
    props_ddl = ",\n  ".join(f"'{k}'='{v}'" for k, v in table_props.RAW.items())
    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.TABLE_ENRICHED} (
  {cols_ddl}
) USING iceberg
PARTITIONED BY (dt)
TBLPROPERTIES (
  {props_ddl}
)
""")
    spark.sparkContext.clearJobTags()


def _enriched_ddl() -> list[str]:
    types = {
        "transaction_id": "string", "event_ts": "string", "event_ts_ts": "timestamp",
        "dt": "date", "dedup_key": "string", "ingest_ts": "timestamp",
        "source_topic": "string", "source_partition": "int", "source_offset": "bigint",
        "kafka_ts": "timestamp", "raw_json": "string",
        "card_id": "string", "merchant_id": "string", "amount": "double",
        "currency": "string", "channel": "string", "merchant_country": "string",
        "card_present": "boolean", "merchant_category": "string", "is_3ds": "boolean",
        "device_id": "string", "merchant_name": "string",
        "merchant_risk_score": "double", "merchant_avg_ticket": "double", "merchant_closed": "boolean",
        "customer_id": "string", "issuer_country": "string", "credit_limit": "double",
        "txn_limit_1h": "double", "travel_notice": "boolean", "card_age_days": "int",
        "customer_segment": "string", "card_status": "string",
        "country_mismatch": "boolean", "first_seen_ts": "timestamp",
        "is_new_merchant_category": "boolean", "ratio_to_merchant_avg": "double",
        "ratio_to_hourly_limit": "double", "txn_count_24h_prior": "int",
        "amount_sum_24h_prior": "double", "merchant_closed_hit": "boolean",
    }
    return [f"{c} {types.get(c, 'string')}" for c in enrichment.enriched_columns()]


def process_batch(spark, micro: DataFrame, batch_id: int, *, dry_run: bool = False) -> dict:
    """Everything this job does with one micro-batch — one plain function.

    Splitting it out of the streaming plumbing is what makes it unit-testable
    (`tests/unit/test_ingestion.py` feeds it a local SparkSession).
    """
    parsed = schema.parse_transactions(micro)
    parsed = parsed.withColumn("raw_json", F.coalesce(F.col("raw_json"), F.col("payload_json")))
    good, bad = schema.split_good_and_bad(parsed)
    good = schema.dedupe(good).drop("qc_failed", "qc_reasons", "payload_json")

    stats = {"batch": batch_id, "rows": micro.count(), "good": good.count()}

    if dry_run:
        return stats

    # (1) raw records: MERGE on the dedup key => replay-safe append
    if stats["good"]:
        sparkutils.merge_batch(
            spark, config.TABLE_RAW, good, ["dedup_key"],
        )
        # (2) enriched records for the feature store (join with dimensions)
        enriched = enrichment.enrich_batch(spark, good)
        sparkutils.append_batch(spark, config.TABLE_ENRICHED,
                                enriched.select(*enrichment.enriched_columns()),
                                props=table_props.RAW)
    # (3) quarantine: never throw data away, park it and alert on it
    quarantined = schema.quarantine_rows(bad).withColumn("dt", F.to_date(F.col("kafka_ts")))
    if quarantined.rdd.isEmpty():
        return stats
    sparkutils.append_batch(spark, config.TABLE_LOAD_FAILURES, quarantined,
                            props=table_props.QUARANTINE)
    stats["quarantined"] = quarantined.count()
    return stats


def main(argv=None) -> int:
    parser = cli.build_parser(JOB)
    args = parser.parse_args(argv)
    if cli.maybe_print_config(args):
        return 0
    cli.announce(JOB, args)

    spark = sparkutils.get_spark(JOB, extra_conf={"spark.sql.streaming.multipleWatermarkPolicy": "min",
                                                  "spark.log.level": cli.spark_log_level(args)})
    build_raw_table(spark)
    schema.ensure_namespaces(spark)

    stream = sparkutils.kafka_source(
        spark, config.KAFKA_TOPIC_RAW, starting=cli.parse_offsets(args.starting),
        max_offsets=args.max_rows_per_trigger,
    )
    checkpoint = f"{config.WAREHOUSE}/checkpoints/{JOB}"

    def sink(spark_, micro, batch_id):
        if micro.rdd.isEmpty():
            return
        stats = process_batch(spark_, micro, batch_id, dry_run=args.dry_run)
        print(f"[{JOB}] {stats}", flush=True)

    sparkutils.start_query(
        stream, name=JOB, checkpoint=checkpoint, sink=sink, once=args.once,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
