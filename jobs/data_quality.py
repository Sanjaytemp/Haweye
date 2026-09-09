"""Data-quality checks over the lakehouse tables (called by Airflow and by CI).

Checks that actually catch real incidents in a streaming lakehouse:
  * freshness   - did the newest event time / commit get too old?  (silent stalls)
  * volume      - rows today vs the trailing average                (pipeline broke)
  * duplicates  - the idempotency key is still unique               (replay storm)
  * null rate   - a critical column went NULL                       (upstream drift)
  * reference   - every fact joins to a dimension                   (missing CDC)

Exit code 1 on any FAILED check so Airflow alerts; results are also appended to
`marts.data_quality_runs` so you can trend them.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F  # noqa: E402

from common import cli, config, sparkutils  # noqa: E402

JOB = "data_quality"

#: (table, check, parameter, severity)  severity: error -> exit 1
CHECKS = [
    (config.TABLE_RAW, "freshness_event_ts", {"max_lag_minutes": 90}, "error"),
    (config.TABLE_RAW, "duplicate_rate", {"key": "dedup_key", "max": 0.0}, "error"),
    (config.TABLE_RAW, "null_rate", {"cols": ["transaction_id", "card_id", "amount", "event_ts_ts"], "max": 0.001}, "error"),
    (config.TABLE_ENRICHED, "reference_integrity", {"fact": "merchant_id", "dim": config.TABLE_MERCHANT_DIM,
                                                    "dim_key": "merchant_id", "max_missing": 0.05}, "warn"),
    (config.TABLE_ENRICHED, "volume_ratio", {"days": 7, "min": 0.2, "max": 5.0}, "warn"),
    (config.TABLE_FEATURES, "freshness_event_ts", {"max_lag_minutes": 120}, "error"),
    (config.TABLE_FEATURES, "null_rate", {"cols": ["amount", "txn_count_5min", "txn_count_1h"],
                                         "max": 0.02}, "error"),
    (config.TABLE_MERCHANT_DIM, "row_count", {"min": 1}, "error"),
    (config.TABLE_CARD_DIM, "row_count", {"min": 1}, "error"),
]


def run_check(spark, table: str, check: str, params: dict) -> dict:
    if not sparkutils.table_exists(spark, table):
        return {"status": "skip", "detail": "table missing"}
    df = spark.table(table)
    n = df.count()
    if check == "row_count":
        return {"rows": n, "status": "pass" if n >= params.get("min", 1) else "fail"}
    if n == 0:
        return {"rows": 0, "status": "fail", "detail": "empty table"}
    if check == "freshness_event_ts":
        col = params.get("column", "event_ts_ts")
        if col not in df.columns:
            return {"status": "skip"}
        latest = df.agg(F.max(col)).first()[0]
        lag_min = spark.sql(
            f"SELECT (unix_timestamp() - unix_timestamp('{latest}')) / 60").first()[0] if latest else None
        ok = lag_min is not None and lag_min <= params["max_lag_minutes"]
        return {"latest_event": str(latest), "lag_minutes": round(float(lag_min or -1), 1),
                "status": "pass" if ok else "fail"}
    if check == "duplicate_rate":
        key = params["key"]
        dup = df.groupBy(key).count().agg(F.sum(F.when(F.col("count") > 1, F.col("count") - 1))).first()[0] or 0
        rate = dup / n
        return {"duplicates": int(dup), "rate": round(rate, 6),
                "status": "pass" if rate <= params["max"] else "fail"}
    if check == "null_rate":
        worst, detail = 0.0, {}
        for c in params["cols"]:
            if c not in df.columns:
                continue
            r = df.agg(F.avg(F.when(F.col(c).isNull(), 1.0).otherwise(0.0))).first()[0] or 0.0
            detail[c] = round(float(r), 6)
            worst = max(worst, float(r))
        return {"null_rates": detail, "status": "pass" if worst <= params["max"] else "fail"}
    if check == "reference_integrity":
        dim = spark.table(params["dim"])
        missing = (df.join(dim.select(F.col(params["dim_key"]).alias("_k")),
                           df[params["fact"]] == dim[params["dim_key"]], "left_anti")
                   .count())
        rate = missing / n
        return {"missing": int(missing), "rate": round(rate, 4),
                "status": "pass" if rate <= params["max_missing"] else "warn" if params.get("soft") else "fail"}
    if check == "volume_ratio":
        days = params.get("days", 7)
        if "dt" not in df.columns:
            return {"status": "skip"}
        per_day = (df.groupBy("dt").count().orderBy(F.col("dt").desc()).limit(days + 1)
                   .toPandas())
        if len(per_day) < 2:
            return {"status": "skip", "detail": "not enough days yet"}
        today, rest = per_day.iloc[0]["count"], per_day.iloc[1:]["count"].mean()
        ratio = float(today) / float(rest) if rest else 0.0
        return {"today": int(today), "avg_prev": round(float(rest), 1), "ratio": round(ratio, 3),
                "status": "pass" if params["min"] <= ratio <= params["max"] else "warn"}
    return {"status": "skip", "detail": f"unknown check {check}"}


def main(argv=None) -> int:
    parser = cli.build_parser(JOB, extra=[
        (("--only-table",), {"default": None}),
        (("--write-results",), {"action": "store_true", "help": "append to marts.data_quality_runs"}),
    ])
    args = parser.parse_args(argv)
    if cli.maybe_print_config(args):
        return 0
    cli.announce(JOB, args)

    spark = sparkutils.get_spark(JOB)
    results, failed, warned = [], 0, 0
    for table, check, params, severity in CHECKS:
        if args.only_table and args.only_table not in table:
            continue
        outcome = run_check(spark, table, check, params)
        row = {"table": table, "check": check, **outcome}
        if outcome.get("status") == "fail" and severity == "error":
            failed += 1
        elif outcome.get("status") in {"fail", "warn"}:
            warned += 1
        results.append(row)
        print(json.dumps(row, default=str), flush=True)

    if args.write_results and not args.dry_run:
        write_results(spark, results)
    print(json.dumps({"checks": len(results), "failed": failed, "warned": warned}, default=str), flush=True)
    return 1 if failed else 0


def write_results(spark, results: list[dict]) -> None:
    import pandas as pd

    table = f"{config.ICEBERG_CATALOG}.{config.ICEBERG_NS_MARTS}.data_quality_runs"
    df = spark.createDataFrame(pd.DataFrame([
        {**r, "run_at": _now(),
         "detail": json.dumps({k: v for k, v in r.items()
                               if k not in ("table", "check", "status")}, default=str)}
        for r in results]))
    (df.select("run_at", "table", "check", "status", "detail")
       .write.format("iceberg").mode("append").saveAsTable(table))


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


if __name__ == "__main__":
    raise SystemExit(main())
