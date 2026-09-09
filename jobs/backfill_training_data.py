"""BATCH JOB — build the training dataset in the lakehouse (labelled history).

Why a separate "backfill" job at all?  Because a brand-new streaming pipeline
has no history: you cannot train a fraud model on 4 minutes of live traffic.
This job creates 30 days of realistic synthetic history and runs **exactly the
same enrichment + feature SQL** the streaming job uses — that is what makes the
model's training data comparable to what it will see in production.

    1. dimensions: Postgres (source of truth)  -> Iceberg dim.*
    2. events:    synthetic history            -> raw.transactions_raw
                 enrichment                     -> raw.transactions_enriched
    3. features:  exact window path            -> features.transactions_feature_v1
    4. labels:    the ground-truth flag that travelled with each event
                 (from the payload, or from Postgres) -> raw.fraud_labels
    5. (optional) rebuild the rolling-aggregate tables the streamer reads

Usage
    ./jobs/submit/run_job.sh backfill_training_data
    python jobs/backfill_training_data.py --days 7 --limit-per-day 5000 --with-stream
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

LABEL_ROWS: list[dict] = []

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F  # noqa: E402

from common import cli, config, dimensions, enrichment, features, schema, sparkutils  # noqa: E402
from common import io as hio  # noqa: E402

JOB = "backfill_training_data"


def _generator_path() -> str:
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(here, "generator")


def generate_history(days: int, rate_per_day: int, seed: int):
    """Import the simulator (pure python) and produce labelled events."""
    sys.path.insert(0, _generator_path())
    from simulator import generate_transactions  # generator/simulator.py

    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    events, labels = generate_transactions(start=start, end=end, n=days * rate_per_day, seed=seed)
    global LABEL_ROWS
    LABEL_ROWS = labels
    return events


def _align_to_dim_table(spark, df, table: str):
    """A Postgres row-set -> exactly what `dim.<table>` and the CDC merge expect.

    Two things this has to get right, and both fail quietly if you don't:

    * **the column set.** Only columns the source has, in the lakehouse table's
      order; if the repo gains a dimension column and the Postgres table wasn't
      re-run, we say so here instead of MERGE-ing a half-populated row.
    * **the CDC bookkeeping.** ``merge_sql`` gates every update on
      ``s.source_ts_ms >= coalesce(t.source_ts_ms, 0)`` and deletes on
      ``s.op = 'DELETE'``. A seed row therefore has to arrive with a timestamp
      (taken from ``updated_at``) and ``op='c'``, or the very first MERGE throws
      ``Column source_ts_ms not found`` and the table is left half-written for the
      real CDC stream to trip over.
    """
    spec = dimensions.TABLES[table]
    missing = [c[0] for c in spec["columns"] if c[0] not in df.columns]
    if missing:
        raise SystemExit(
            f"public.{table} has no column(s) {missing} - run `make seed-dims` / "
            f"apply sql/10_dimensions.sql so the source matches jobs/common/dimensions.py")
    # select the business columns in the spec's order, then attach the bookkeeping
    out = df.select(*[c[0] for c in spec["columns"]])
    out = (out
           .withColumn("source_ts_ms", (F.unix_timestamp(F.col("updated_at").cast("timestamp")) * 1000)
                       .cast("bigint"))
           .withColumn("source_ts", (F.col("source_ts_ms") / 1000).cast("timestamp"))
           .withColumn("op", F.lit("c"))
           .withColumn("source_db", F.lit(config.CDC_PG_DB))
           .withColumn("source_table", F.lit(table))
           .withColumn("ingest_ts", F.current_timestamp()))
    # the frame now carries exactly spec columns + cdc_mod.MANAGED_COLUMNS, which is
    # what the CDC merge writes; write_dimensions() below still intersects with the
    # live table's schema, so an older table missing a business column survives.
    return out


def write_dimensions(spark) -> None:
    """Postgres (source of truth) -> Iceberg `dim.*`, idempotently.

    First run inserts; later runs upsert only *newer* rows, which is exactly the
    contract the CDC job will use once you switch it on (docs/05-cdc.md).
    """
    schema.ensure_namespaces(spark)
    for table, spec in dimensions.TABLES.items():
        fqn = f"{config.ICEBERG_CATALOG}.dim.{table}"
        data_cols = [f.name for f in spark.table(fqn).schema.fields] if sparkutils.table_exists(spark, fqn) else None
        rows = hio.read_table(f"SELECT * FROM public.{table}")
        if not rows:
            print(f">>> public.{table} is empty - run `make seed-dims` first", flush=True)
            continue
        payload = _align_to_dim_table(spark, spark.createDataFrame(rows), table)
        if data_cols is None:
            payload.write.format("iceberg").mode("errorifexists") \
                .option("write.format.default", "parquet").saveAsTable(fqn)
            print(f">>> created {fqn} with {len(rows)} rows", flush=True)
            continue
        payload = payload.select(*[c for c in payload.columns if c in data_cols])
        payload.createOrReplaceTempView(f"dim_seed_{table}")
        updates = ", ".join(f"t.{c} = s.{c}" for c in data_cols if c != spec["pk"])
        spark.sql(f"""
MERGE INTO {fqn} t
USING dim_seed_{table} s
ON t.{spec['pk']} = s.{spec['pk']}
WHEN MATCHED AND s.source_ts_ms >= coalesce(t.source_ts_ms, 0) THEN UPDATE SET {updates}
WHEN NOT MATCHED THEN INSERT ({", ".join(data_cols)}) VALUES ({", ".join('s.' + c for c in data_cols)})
""")
        print(f">>> upserted {len(rows)} rows -> {fqn}", flush=True)


def write_events(spark, events) -> int:
    """DataFrame of generated transactions -> raw + enriched tables."""
    import pandas as pd

    pdf = pd.DataFrame(events)
    pdf["event_ts_ts"] = pd.to_datetime(pdf["event_ts_ts"])
    pdf = pdf.drop(columns=[c for c in ("label", "fraud_type") if c in pdf.columns])
    df = spark.createDataFrame(pdf)
    df = (df.withColumn("dt", F.to_date(F.col("event_ts_ts")))
            .withColumn("dedup_key", F.md5(F.col("transaction_id")))
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("source_topic", F.lit("backfill"))
            .withColumn("source_partition", F.lit(0))
            .withColumn("source_offset", F.monotonically_increasing_id())
            .withColumn("kafka_ts", F.col("event_ts_ts"))
            .withColumn("raw_json", F.to_json(F.struct("transaction_id", "event_ts", "card_id",
                                                       "merchant_id", "amount"))))
    raw_cols = [f.name for f in spark.table(config.TABLE_RAW).schema.fields]
    df.select(*[F.coalesce(F.col(c), F.lit(None)).alias(c) if c in df.columns
                else F.lit(None).alias(c) for c in raw_cols]) \
      .write.format("iceberg").mode("append").saveAsTable(config.TABLE_RAW)

    enriched_cols = enrichment.enriched_columns()
    enriched = enrichment.enrich_batch(spark, df)
    enriched.select(*[F.col(c) if c in enriched.columns else F.lit(None).alias(c) for c in enriched_cols]) \
        .write.format("iceberg").mode("append").saveAsTable(config.TABLE_ENRICHED)
    return df.count()


def write_features_and_labels(spark, *, rebuild_state_only: bool = False) -> dict:
    """Exact feature path over the whole enriched history + the label table."""
    enriched = spark.table(config.TABLE_ENRICHED)
    n = enriched.count()
    if n == 0:
        return {"rows": 0}

    # the batch path computes the *exact* rolling windows (history is all here)
    feats = features.compute_features(enriched)
    feats = feats.withColumn("feature_path", F.lit("backfill"))
    layout = features.to_feature_store_layout(feats, model_uri="pending", model_version="pending")
    cols = [f.name for f in features.FEATURES_TABLE_SCHEMA.fields]
    if not sparkutils.table_exists(spark, config.TABLE_FEATURES):
        (layout.write.format("iceberg").mode("errorifexists")
         .partitionedBy("dt")
         .option("write.format.default", "parquet")
         .saveAsTable(config.TABLE_FEATURES))
    else:
        sparkutils.merge_batch(spark, config.TABLE_FEATURES, layout.select(*cols), ["transaction_id"],
                              update_cols=[c for c in cols if c not in ("transaction_id", "dt")])

    labelled = _label_frame(spark)
    if labelled is None:
        print(">>> no ground-truth labels found (run the backfill with labels in the payload)", flush=True)
        return {"rows": n, "labels": 0}
    if not sparkutils.table_exists(spark, config.TABLE_LABELS):
        (labelled.withColumn("dt", F.lit(None).cast("date"))
         .write.format("iceberg").mode("errorifexists")
         .partitionedBy("dt").option("write.format.default", "parquet")
         .saveAsTable(config.TABLE_LABELS))
    else:
        sparkutils.merge_batch(spark, config.TABLE_LABELS, labelled, ["transaction_id"],
                              update_cols=["label", "fraud_type"])

    state = rebuild_card_state(spark) if rebuild_state_only else None
    return {"rows": n, "labels": labelled.count(), "fraud_rate":
            round(labelled.agg(F.avg("label")).first()[0] or 0.0, 5), "state": state}


def _label_frame(spark):
    """Ground truth comes either from the payload we backfilled or from Postgres.

    In production these labels arrive days later from the disputes/chargeback
    system - which is why they live in their own table and the training DAG joins
    them at read time instead of baking them into the feature table (a feature
    table that leaks its own target is the classic fraud-model blunder).
    """
    import pandas as pd

    rows = LABEL_ROWS
    if rows:
        return spark.createDataFrame(pd.DataFrame(rows)).select(
            "transaction_id", F.col("label").cast("double").alias("label"),
            F.col("fraud_type").cast("string").alias("fraud_type"))
    try:
        data = hio.read_table("SELECT transaction_id, label, fraud_type FROM public.transaction_labels")
    except Exception:
        return None
    if not data:
        return None
    return spark.createDataFrame(pd.DataFrame(data)).select(
        "transaction_id", F.col("label").cast("double").alias("label"),
        F.col("fraud_type").cast("string").alias("fraud_type"))


def rebuild_card_state(spark) -> dict:
    """Rebuild `raw.card_minute_agg` / `raw.card_day_agg` from the enriched table."""
    spark.table(config.TABLE_ENRICHED).createOrReplaceTempView("state_source")
    out = {}
    for name, sql in (("minute", features.minute_agg_sql("state_source")),
                      ("day", features.day_agg_sql("state_source"))):
        table = f"{config.ICEBERG_CATALOG}.{features.DAY_TABLES[name]}"
        if not sparkutils.table_exists(spark, table):
            spark.sql(f"CREATE TABLE {table} USING iceberg AS SELECT * FROM ({sql})")
            out[name] = "created"
        else:
            # full rebuild of derived state: cheap and always correct
            spark.sql(f"INSERT INTO {table} SELECT * FROM ({sql})")
            out[name] = "appended"
    return out


def main(argv=None) -> int:
    parser = cli.build_parser(JOB, extra=[
        (("--days",), {"type": int, "default": 30}),
        (("--rows-per-day",), {"type": int, "default": 20_000}),
        (("--seed",), {"type": int, "default": int(os.environ.get("GEN_SEED", 20240601))}),
        (("--skip-events",), {"action": "store_true", "help": "only (re)build features/labels"}),
        (("--rebuild-state-only",), {"action": "store_true"}),
        (("--with-stream",), {"action": "store_true", "help": "also publish history to Kafka"})])
    args = parser.parse_args(argv)
    if cli.maybe_print_config(args):
        return 0
    cli.announce(JOB, args)

    spark = sparkutils.get_spark(JOB, extra_conf={
        "spark.sql.shuffle.partitions": os.environ.get("SHUFFLE_PARTITIONS", "8")})
    schema.ensure_namespaces(spark)
    import streaming_ingestion as sj

    sj.build_raw_table(spark)
    write_dimensions(spark)
    if args.rebuild_state_only:
        print(write_features_and_labels(spark, rebuild_state_only=True))
        return 0
    if not args.skip_events:
        events = generate_history(args.days, args.rows_per_day, args.seed)
        rows = write_events(spark, events)
        print(f">>> wrote {rows} synthetic transactions", flush=True)
        if args.with_stream:
            _publish_to_kafka(spark, events)
    print(write_features_and_labels(spark))
    return 0


def _publish_to_kafka(spark, events) -> None:
    """Replay the history into Kafka so the streaming jobs can be observed live."""
    import json as _json

    import pandas as pd

    payload = [{"key": e["card_id"], "value": _json.dumps({k: v for k, v in e.items() if k != "label"})}
               for e in events]
    df = spark.createDataFrame(pd.DataFrame(payload))
    (df.write.format("kafka")
       .option("kafka.bootstrap.servers", config.KAFKA_SERVERS)
       .option("topic", config.KAFKA_TOPIC_RAW)
       .save())


if __name__ == "__main__":
    raise SystemExit(main())
