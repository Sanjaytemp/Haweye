"""Iceberg table maintenance — the job that keeps a lakehouse a *lakehouse*.

Streaming writes small files every 10 seconds.  Left alone, a table with 100k
tiny files becomes slow and bloated; snapshots accumulate; orphaned parquet
files from failed jobs never disappear.  Three commands fix all of that, and
they are exactly what an Airflow DAG should run nightly:

    rewrite_data_files   -> bin-pack small files into ~128MB ones
    expire_snapshots     -> drop metadata/versions older than N days
    remove_orphan_files  -> delete files no snapshot references

    python jobs/table_maintenance.py --tables all --older-than-days 7
    python jobs/table_maintenance.py --only features.transactions_feature_v1 --compact
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import cli, config, sparkutils  # noqa: E402

JOB = "table_maintenance"


def all_tables(spark) -> list[str]:
    out = []
    for ns in (config.ICEBERG_NS_RAW, config.ICEBERG_NS_DIM, config.ICEBERG_NS_FEATURES,
               config.ICEBERG_NS_MARTS):
        fqn = f"{config.ICEBERG_CATALOG}.{ns}"
        try:
            for row in spark.sql(f"SHOW TABLES IN {fqn}").collect():
                out.append(f"{fqn}.{row.tableName}")
        except Exception as exc:
            print(f">>> skipping {fqn}: {exc}", flush=True)
    return out


def compact(spark, table: str, target_bytes: int) -> None:
    spark.sql(f"CALL {config.ICEBERG_CATALOG}.system.rewrite_data_files("
              f"table => '{table}', options => map('target-file-size-bytes','{target_bytes}'))")


def expire(spark, table: str, older_than_days: int, retain_last: int = 5) -> None:
    spark.sql(f"CALL {config.ICEBERG_CATALOG}.system.expire_snapshots("
              f"table => '{table}', older_than => TIMESTAMP '{_ago(older_than_days)}', "
              f"retain_last => {retain_last})")


def orphans(spark, table: str, older_than_days: int = 3) -> None:
    spark.sql(f"CALL {config.ICEBERG_CATALOG}.system.remove_orphan_files("
              f"table => '{table}', older_than => TIMESTAMP '{_ago(older_than_days)}')")


def analyze(spark, table: str) -> None:
    spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS")


def _ago(days: int) -> str:
    from datetime import datetime, timedelta, timezone

    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:00")


def report(spark, table: str) -> dict:
    files = spark.sql(f"SELECT count(*) c, sum(file_size_in_bytes) b FROM {table}.files").first()
    snaps = spark.sql(f"SELECT count(*) c FROM {table}.snapshots").first()
    return {"table": table, "files": files["c"], "bytes": files["b"],
            "snapshots": snaps["c"],
            "rows": spark.table(table).count() if sparkutils.table_exists(spark, table) else None}


def main(argv=None) -> int:
    parser = cli.build_parser(JOB, extra=[
        (("--only",), {"action": "append", "default": [], "help": "restrict to a table (repeatable)"}),
        (("--older-than-days",), {"type": int, "default": 7}),
        (("--orphan-days",), {"type": int, "default": 3}),
        (("--target-file-mb",), {"type": int, "default": 128}),
        (("--skip-compact",), {"action": "store_true"}),
        (("--skip-expire",), {"action": "store_true"}),
        (("--skip-orphans",), {"action": "store_true"}),
        (("--analyze",), {"action": "store_true"}),
        (("--report-only",), {"action": "store_true"})])
    args = parser.parse_args(argv)
    if cli.maybe_print_config(args):
        return 0
    cli.announce(JOB, args)

    spark = sparkutils.get_spark(JOB)
    tables = args.only or all_tables(spark)
    results = []
    for table in tables:
        row = {"table": table, "actions": []}
        try:
            if args.report_only:
                row.update(report(spark, table))
                results.append(row)
                continue
            if not args.skip_compact:
                compact(spark, table, args.target_file_mb * 1024 * 1024)
                row["actions"].append("rewrite_data_files")
            if not args.skip_expire:
                expire(spark, table, args.older_than_days)
                row["actions"].append(f"expire_snapshots({args.older_than_days}d)")
            if not args.skip_orphans:
                orphans(spark, table, args.orphan_days)
                row["actions"].append("remove_orphan_files")
            if args.analyze:
                analyze(spark, table)
                row["actions"].append("analyze")
            row.update(report(spark, table))
        except Exception as exc:
            row["error"] = f"{type(exc).__name__}: {exc}"
        results.append(row)
        print(json.dumps(row, default=str), flush=True)
    print(json.dumps({"maintained": len(results)}, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
