"""JOB 3 / 3 — real-time scoring: features -> model + rules -> alerts.

    Kafka `transactions_features`  (published by the feature job)
      -> decode with the feature-store schema   (types come from ONE definition)
      -> VectorUDT in the trained order -> PipelineModel.transform()
      -> deterministic rules (velocity, abroad, ...) in parallel
      -> final_score = 0.75*model + 0.25*rules (rules alone when no model yet),
        decision, is_alert
      -> Postgres (public.fraud_scores / fraud_alerts)   <- alert console, case mgmt
      -> Redis    (score:txn:<id>)                       <- the API reads this first
      -> Iceberg  (marts.transaction_scores)              <- audit + retraining

The model is (re)loaded at most every `--model-refresh-batches` triggers by
reading `models/fraud_rf/version.txt`, so a nightly retrain is picked up without
restarting the stream — and if the model is missing/invalid the job still scores
with rules only (a pipeline is never "down" because ML is).
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import cli, config, features, model, rules, sparkutils  # noqa: E402
from common import io as hio

JOB = "real_time_scoring"
_MODEL_CACHE: dict[str, object] = {"version": None, "pipeline": None, "batches": 0}


def current_pipeline(spark, refresh_every: int = 10):
    """Load the registered pipeline only when the version pointer changed."""
    info = model.read_current_version(spark) or {"version": None}
    if info.get("version") == _MODEL_CACHE["version"] and _MODEL_CACHE["pipeline"] is not None:
        _MODEL_CACHE["batches"] += 1
        if _MODEL_CACHE["batches"] < refresh_every:
            return _MODEL_CACHE["pipeline"], info
    pipeline, status = model.load_pipeline_model(spark)
    _MODEL_CACHE.update(version=info.get("version"), pipeline=pipeline, batches=0, status=status)
    print(f"[{JOB}] model: {status}", flush=True)
    return pipeline, info


def score_batch(spark, feats: DataFrame, *, dry_run: bool = False) -> tuple[DataFrame, dict]:
    """Full scoring logic for one micro-batch (unit-testable without streaming)."""
    feats = features.ensure_feature_columns(feats, include_label=False)
    pipeline, info = current_pipeline(spark)

    if pipeline is None:
        scored = (feats.withColumn("probability", F.lit(None).cast("array<double>"))
                  .withColumn("prediction", F.lit(None).cast("double")))
        status = {"model": "absent", "reason": "no version.txt pointer; rules only"}
    else:
        try:
            transformed = pipeline.transform(feats)
            prob_col = "probability" if "probability" in transformed.columns else None
            scored = transformed.withColumn(
                "prediction",
                (F.element_at(F.col(prob_col), 2) if prob_col else F.col("prediction")).cast("double"),
            )
            status = {"model": info.get("version"), "reason": "loaded"}
        except Exception as exc:
            scored = feats.withColumn("prediction", F.lit(None).cast("double"))
            status = {"model": "error", "reason": f"{type(exc).__name__}: {exc}"}

    ruled = rules.apply_rules(scored)
    threshold, alert_floor = config.SCORE_THRESHOLD, config.ALERT_MIN_SCORE
    # one definition of the blend, in rules.py, expressed as SQL
    blended = F.expr(rules.blend_sql("coalesce(prediction, 0)", "coalesce(rule_score, 0)"))
    final = (
        ruled
        .withColumn("final_score", F.when(F.col("prediction").isNull(),
                                         F.greatest(F.lit(0.0), F.coalesce(F.col("rule_score"), F.lit(0.0))))
                                 .otherwise(blended))
        .withColumn(
            "decision",
            F.when(F.col("final_score") >= F.lit(alert_floor), F.lit("decline"))
             .when(F.col("final_score") >= F.lit(threshold), F.lit("review"))
             .when(F.col("final_score") >= F.lit(threshold * 0.6), F.lit("monitor"))
             .otherwise(F.lit("approve")),
        )
        .withColumn("is_alert", F.col("final_score") >= F.lit(threshold))
        .withColumn("scored_at", F.current_timestamp())
        .withColumn("model_version", F.lit(str(info.get("version") or "none")))
    )

    stats = {"rows": final.count(), **status}
    if dry_run:
        return final, stats
    write_serving_rows(spark, final)
    return final, stats


def write_serving_rows(spark, final: DataFrame) -> None:
    """Iceberg (audit) + Postgres (ops) + Redis (low latency) — same numbers."""
    cols = [c for c in (
        "transaction_id", "card_id", "customer_id", "event_ts_ts", "amount", "prediction",
        "rule_score", "final_score", "decision", "rule_hits", "is_alert", "model_version", "scored_at",
    ) if c in final.columns]
    marts = f"{config.ICEBERG_CATALOG}.{config.ICEBERG_NS_MARTS}.transaction_scores"
    if not sparkutils.table_exists(spark, marts):
        (final.select(*cols)
              .withColumn("dt", F.to_date(F.col("event_ts_ts")))
              .write.format("iceberg").partitionedBy("dt")
              .option("write.format.default", "parquet")
              .mode("errorifexists").saveAsTable(marts))
    else:
        sparkutils.merge_batch(spark, marts, final.select(*cols, F.to_date(F.col("event_ts_ts")).alias("dt")),
                              ["transaction_id"], update_cols=[c for c in cols if c != "transaction_id"])

    payload_cols = cols + ["feature_snapshot"]
    snapshot_cols = [c for c in hio.FEATURE_COLUMNS_FOR_SERVING if c in final.columns]
    rows = final.withColumn(
        "feature_snapshot",
        F.to_json(F.struct(*[F.col(c) for c in snapshot_cols])),
    ).select(*payload_cols)

    scores = []
    for r in rows.collect():
        d = r.asDict()
        d["model_score"] = d.pop("prediction", None)
        scores.append(d)
    hio.write_predictions(scores)
    hio.write_score_to_redis(scores)


def main(argv=None) -> int:
    parser = cli.build_parser(JOB, extra=[
        (("--model-refresh-batches",), {"type": int, "default": 10}),
        (("--no-serving",), {"action": "store_true", "help": "skip Postgres/Redis writes (Iceberg only)"}),
        (("--no-iceberg",), {"action": "store_true", "help": "skip the marts table (useful in dev)"}),
    ])
    args = parser.parse_args(argv)
    if cli.maybe_print_config(args):
        return 0
    cli.announce(JOB, args)

    spark = sparkutils.get_spark(JOB)
    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", config.KAFKA_SERVERS)
              .option("subscribe", config.KAFKA_TOPIC_FEATURES)
              .option("startingOffsets", cli.parse_offsets(args.starting))
              .option("failOnDataLoss", "false")
              .load())

    def sink(spark_, micro, batch_id):
        if micro.rdd.isEmpty():
            return
        feats = features.parse_feature_json(micro)
        final, stats = score_batch(spark_, feats, dry_run=args.dry_run)
        stats["batch"] = batch_id
        print(f"[{JOB}] {json.dumps(stats, default=str)}", flush=True)

    sparkutils.start_query(stream, name=JOB,
                           checkpoint=f"{config.WAREHOUSE}/checkpoints/{JOB}",
                           sink=sink, once=args.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
