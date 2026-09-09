"""CDC (one-shot batch) — merge dimension changes into Iceberg and exit.

Same code path as `cdc_merge_stream.py`, wrapped in the `once` trigger semantics so
Airflow can schedule it every 5 minutes instead of running an always-on stream.
Which one you pick is a pure cost/latency trade-off (docs/05-cdc.md):

    stream:   ~seconds of staleness, one always-on Spark job
    batch:    ~5 minutes of staleness, jobs come and go, easier to backfill

Also demonstrates the *other* CDC direction — reading only what changed in an
Iceberg table since the last run (`--from-lake <table>`), which is how a
downstream aggregate can stay incremental instead of recomputing everything.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import cdc, cli, config, schema, sparkutils  # noqa: E402
from common import cdc_merge_common as common  # noqa: E402

JOB = "cdc_merge_batch"


def main(argv=None) -> int:
    parser = cli.build_parser(JOB, extra=[
        (("--from-lake",), {"default": None,
                            "help": "instead of Kafka, read changes of an Iceberg table since last snapshot"}),
        (("--snapshot-state",), {"default": "record", "help": "record|ignore - keep the state table up to date"}),
        (("--mirror-postgres",), {"action": "store_true"}),
    ])
    args = parser.parse_args(argv)
    if cli.maybe_print_config(args):
        return 0
    cli.announce(JOB, args)

    spark = sparkutils.get_spark(JOB)
    schema.ensure_namespaces(spark)
    from common import dimensions

    for ddl in dimensions.iceberg_ddl(config.ICEBERG_CATALOG).split("\n\n"):
        if ddl.strip():
            spark.sql(ddl)

    if args.from_lake:
        table = args.from_lake if "." in args.from_lake else f"{config.ICEBERG_CATALOG}.{args.from_lake}"
        start = cdc.last_applied_snapshot(spark, table)
        end = cdc.current_snapshot_id(spark, table)
        changed = cdc.read_incremental(spark, table, start, end)
        n = changed.count()
        print(json.dumps({"table": table, "start_snapshot": start, "end_snapshot": end, "rows": n},
                         default=str), flush=True)
        if args.dry_run:
            return 0
        if n:
            mode = "overwrite" if start is None else "append"   # first run seeds the whole table
            changed.write.format("iceberg").mode(mode).saveAsTable(_shadow(table))
        if args.snapshot_state == "record":
            cdc.record_snapshot(spark, table, end)
        return 0

    bounded = common.read_cdc(spark, streaming=False, starting=args.starting)
    stats = common.apply_changes(spark, bounded, dry_run=args.dry_run)
    for table in (config.TABLE_MERCHANT_DIM, config.TABLE_CARD_DIM):
        cdc.record_snapshot(spark, table, cdc.current_snapshot_id(spark, table))
    if args.mirror_postgres and not args.dry_run:
        stats["mirrored_rows"] = common.publish_dimensions_to_serving(spark)
    print(json.dumps(stats, default=str), flush=True)
    return 0


def _shadow(table: str) -> str:
    return table.replace(".dim.", ".raw.") + "_shadow"


if __name__ == "__main__":
    raise SystemExit(main())
