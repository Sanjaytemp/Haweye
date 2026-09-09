"""JOB 2 / 3 — the feature store: rolling features written to Iceberg *and* Redis.

Reads the append-stream of `raw.transactions_enriched` (Iceberg can stream its
own table changes) and per micro-batch:

    1. read "strictly prior" rolling aggregates (minute + day tables)
    2. exact within-batch windows (5 min is fully covered by a 10s batch)
    3. derive ratios / calendar features
    4. MERGE the feature vector into `features.transactions_feature_v1`  <- offline store
    5. update the rolling aggregate tables (minute / day)                <- the trick
    6. push the same vectors to Redis + Postgres                         <- online store
    7. publish a compact feature event to Kafka `transactions_features`  <- for scoring

Why the read-before-write order in (1)/(5) matters: the history we read does
not contain the rows we are about to write, so "prior" windows are correct
without any subtraction games, and a replayed batch produces the same numbers
because every write is a MERGE.

`--verify` re-derives the same features with the *exact* window function over
history (the batch path) and prints the mean absolute difference — a live demo
that streaming and batch agree.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import cli, config, features, schema, sparkutils  # noqa: E402
from common import io as hio

JOB = "feature_store"
RICH_COLS = [f.name for f in features.FEATURES_TABLE_SCHEMA.fields]


def ensure_tables(spark) -> None:
    schema.ensure_namespaces(spark)
    cols_ddl = ",\n  ".join(_ddl_for(f) for f in features.FEATURES_TABLE_SCHEMA.fields)
    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config.TABLE_FEATURES} (
  {cols_ddl}
) USING iceberg
PARTITIONED BY (dt)
TBLPROPERTIES (
  'write.format.default'='parquet',
  'write.distribution-mode'='hash',
  'write.target-file-size-bytes'='134217728'
)
""")
    # The two rolling-aggregate tables are *derived* state: if they are lost you
    # can rebuild them from `raw.transactions_enriched` with one batch query
    # (jobs/backfill_training_data.py --rebuild-state-only).
    for name, ddl in (("minute", features.MINUTE_AGG_SCHEMA), ("day", features.DAY_AGG_SCHEMA)):
        table = features.DAY_TABLES[name]
        cols = ",\n  ".join(_ddl_for(f) for f in ddl.fields)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {config.ICEBERG_CATALOG}.{table} (\n  {cols}\n)"
            " USING iceberg\nPARTITIONED BY (dt)\n"
            "TBLPROPERTIES ('write.format.default'='parquet', 'write.distribution-mode'='none')"
        )


def _ddl_for(field) -> str:
    return f"{field.name} {field.dataType.simpleString()}" + (" NOT NULL" if not field.nullable else "")


# ------------------------------------------------------------------ the logic
def build_features(spark: SparkSession, current: DataFrame) -> DataFrame:  # noqa: F821
    """Steps 1-3.  Returns the feature frame for `current` (already enriched)."""
    current.createOrReplaceTempView("current_batch")
    spark.sql(features.batch_bounds_sql("current_batch")).createOrReplaceTempView("batch_bounds")

    hist_1h = spark.sql(features.minute_history_sql("batch_bounds"))
    hist_24h = spark.sql(features.day_history_sql("current_batch"))

    # 2) exact windows inside the micro-batch
    batch_feats = spark.sql(features.micro_batch_aggs_sql("current_batch"))

    # 1c) "has this card used this merchant category before?" - read before merge
    day_fqn = f"{config.ICEBERG_CATALOG}.{features.DAY_TABLES['day']}"
    cat_rows = (spark.table(day_fqn)
                .select("card_id", F.explode("categories").alias("merchant_category"))
                .distinct()
                .where(F.col("merchant_category").isNotNull())
                if sparkutils.table_exists(spark, day_fqn) else None)

    joined = (
        batch_feats
        .join(hist_1h, "card_id", "left")
        .join(hist_24h, "card_id", "left")
    )
    joined.createOrReplaceTempView("joined_feats")

    if cat_rows is not None:
        cat_rows.createOrReplaceTempView("seen_categories")
        newness = spark.sql("""
SELECT /*+ BROADCAST(sc) */ j.transaction_id,
       sc.merchant_category IS NULL AS is_new_merchant_category
FROM (SELECT DISTINCT transaction_id, card_id, merchant_category FROM joined_feats) j
LEFT JOIN seen_categories sc
       ON sc.card_id = j.card_id AND sc.merchant_category = j.merchant_category
""")
    else:
        newness = spark.sql(
            "SELECT transaction_id, TRUE AS is_new_merchant_category FROM joined_feats "
            "GROUP BY transaction_id"
        )

    enriched = (
        spark.sql(f"SELECT t.*, {features.derived_features_sql()} FROM joined_feats t")
        .join(F.broadcast(newness), "transaction_id", "left")
    )
    out = enriched.withColumn("feature_path", F.lit("streaming"))
    return features.to_feature_store_layout(out, model_version="pending", model_uri="pending")


def update_rolling_tables(spark, current: DataFrame) -> None:
    """Step 5 - merge this batch's aggregates into the rolling tables."""
    current.createOrReplaceTempView("agg_input")
    minute = spark.sql(features.minute_agg_sql("agg_input"))
    day = spark.sql(features.day_agg_sql("agg_input"))
    _merge_minute(spark, minute)
    _merge_day(spark, day)


def _merge_minute(spark, minute: DataFrame) -> None:
    minute.createOrReplaceTempView("m_in")
    spark.sql(f"""
MERGE INTO {config.ICEBERG_CATALOG}.{features.DAY_TABLES['minute']} t
USING m_in s
ON t.card_id = s.card_id AND t.bucket_ts = s.bucket_ts AND t.metric_set = s.metric_set
WHEN MATCHED THEN UPDATE SET
  t.cnt = t.cnt + s.cnt,
  t.amt_sum = t.amt_sum + s.amt_sum,
  t.amt_max = greatest(t.amt_max, s.amt_max),
  t.amt_sq = t.amt_sq + s.amt_sq,
  t.amt_avg = (t.amt_sum + s.amt_sum) / (t.cnt + s.cnt),
  t.distinct_merchants = cardinality(array_distinct(concat(coalesce(t.merchants, array()), s.merchants))),
  t.merchants = slice(array_distinct(concat(coalesce(t.merchants, array()), s.merchants)), 1, 50),
  t.min_ts = least(t.min_ts, s.min_ts),
  t.max_ts = greatest(t.max_ts, s.max_ts)
WHEN NOT MATCHED THEN INSERT *
""")


def _merge_day(spark, day: DataFrame) -> None:
    day.createOrReplaceTempView("d_in")
    spark.sql(f"""
MERGE INTO {config.ICEBERG_CATALOG}.{features.DAY_TABLES['day']} t
USING d_in s
ON t.card_id = s.card_id AND t.dt = s.dt
WHEN MATCHED THEN UPDATE SET
  t.txn_count = t.txn_count + s.txn_count,
  t.amount_sum = t.amount_sum + s.amount_sum,
  t.amount_max = greatest(t.amount_max, s.amount_max),
  t.amount_sq = t.amount_sq + s.amount_sq,
  t.merchants = slice(array_distinct(concat(coalesce(t.merchants, array()), s.merchants)), 1, 50),
  t.categories = slice(array_distinct(concat(coalesce(t.categories, array()), s.categories)), 1, 50),
  t.distinct_merchants = cardinality(array_distinct(concat(coalesce(t.merchants, array()), s.merchants))),
  t.first_seen = least(t.first_seen, s.first_seen),
  t.last_seen = greatest(t.last_seen, s.last_seen)
WHEN NOT MATCHED THEN INSERT *
""")


def publish_features(df: DataFrame) -> None:
    """Step 7 - a compact feature event for downstream consumers (scoring, Flink, ...)."""
    cols = [c for c in RICH_COLS if c in df.columns and c != "feature_path"]
    payload = df.select(F.to_json(F.struct(*[F.col(c) for c in cols])).alias("value"))
    (payload.write.format("kafka")
     .option("kafka.bootstrap.servers", config.KAFKA_SERVERS)
     .option("topic", config.KAFKA_TOPIC_FEATURES)
     .save())


def process_batch(spark, micro: DataFrame, batch_id: int, *, dry_run: bool = False,
                  with_redis: bool = True, publish: bool = True) -> dict:
    enriched = micro.drop("qc_failed", "qc_reasons") if "qc_failed" in micro.columns else micro
    feats = build_features(spark, enriched)
    stats = {"batch": batch_id, "features": feats.count()}
    if dry_run:
        return stats
    sparkutils.merge_batch(spark, config.TABLE_FEATURES, feats.select(*RICH_COLS), ["transaction_id"])
    update_rolling_tables(spark, enriched)
    if publish:
        try:
            publish_features(feats)
        except Exception as exc:  # a Kafka hiccup must not stop the lakehouse write
            print(f"[{JOB}] feature publish failed: {exc}", flush=True)
    if with_redis:
        records = [r.asDict() for r in feats.select(
            *[c for c in hio.FEATURE_COLUMNS_FOR_SERVING if c in feats.columns]).collect()]
        hio.write_features_to_redis(records)
        hio.write_feature_snapshot(feats.select(
            *[c for c in hio.FEATURE_COLUMNS_FOR_SERVING if c in feats.columns]))
    return stats


def main(argv=None) -> int:
    parser = cli.build_parser(JOB, extra=[
        (("--no-redis",), {"action": "store_true", "help": "skip the online store writes"}),
        (("--no-publish",), {"action": "store_true", "help": "skip publishing to Kafka"}),
        (("--source",), {"default": "stream", "help": "stream | snapshot | table:<name>"}),
        (("--verify",), {"action": "store_true", "help": "compare against the exact batch window"})])
    args = parser.parse_args(argv)
    if cli.maybe_print_config(args):
        return 0
    if args.trigger_seconds:
        config.STREAM_TRIGGER_SECONDS = args.trigger_seconds
    cli.announce(JOB, args)

    spark = sparkutils.get_spark(JOB)
    ensure_tables(spark)

    if args.source == "stream":
        stream = sparkutils.iceberg_source(spark, config.TABLE_ENRICHED, starting=args.starting)
    else:
        stream = sparkutils.iceberg_source(spark, args.source.split(":", 1)[1])

    def sink(spark_, micro, batch_id):
        if micro.rdd.isEmpty():
            return
        stats = process_batch(spark_, micro, batch_id, dry_run=args.dry_run,
                              with_redis=not args.no_redis, publish=not args.no_publish)
        if args.verify:
            stats["verify"] = _verify(spark_, micro)
        print(f"[{JOB}] {json.dumps(stats, default=str)}", flush=True)

    sparkutils.start_query(
        stream, name=JOB, checkpoint=f"{config.WAREHOUSE}/checkpoints/{JOB}", sink=sink, once=args.once
    )
    return 0


def _verify(spark, micro: DataFrame) -> dict:
    """Recompute features with the *exact* window path (batch) and diff them.

    This is the cheapest guard against training/serving skew: the same rows, two
    independent computations, average absolute difference per feature.
    """
    current = micro.drop("qc_failed", "qc_reasons") if "qc_failed" in micro.columns else micro
    current.createOrReplaceTempView("verify_current")
    ids = [r[0] for r in spark.sql("SELECT DISTINCT transaction_id FROM verify_current").collect()]
    lo = spark.sql("SELECT min(event_ts_ts) - INTERVAL 25 HOURS AS lo FROM verify_current").first()["lo"]
    hist = spark.table(config.TABLE_ENRICHED).where(F.col("event_ts_ts") >= lo)
    unioned = hist.unionByName(current.select(*hist.columns), allowMissingColumns=True) \
                   .where(sparkutils.isin("transaction_id", ids))
    exact = features.compute_features(unioned).where(sparkutils.isin("transaction_id", ids))
    built = build_features(spark, current)

    diffs = {}
    for colname in ("txn_count_1h", "amount_sum_1h", "amount_sum_24h", "txn_count_5min"):
        e = exact.groupBy("transaction_id").agg(F.max(colname).alias("e")).alias("e")
        b = built.groupBy("transaction_id").agg(F.max(colname).alias("b")).alias("b")
        row = e.join(b, "transaction_id", "inner").select(
            F.avg(F.abs(F.coalesce(F.col("e"), F.lit(0.0)) - F.coalesce(F.col("b"), F.lit(0.0)))).alias("mad")
        ).first()
        diffs[colname] = None if not row or row["mad"] is None else round(float(row["mad"]), 4)
    return {"rows": len(ids), "mean_abs_diff": diffs}


if __name__ == "__main__":
    raise SystemExit(main())
