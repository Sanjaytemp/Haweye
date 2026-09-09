"""CDC (streaming) — keep the Iceberg dimension tables in step with Postgres.

    Debezium -> Kafka (`cdc.public.merchants`, `cdc.public.card_accounts`)
      -> parse envelope -> MERGE INTO dim.* (upsert + delete, replay-safe)
      -> optional mirror to Postgres for the alert console

Run inside compose (needs the CDC stack: `make cdc-up`):
    ./jobs/submit/run_job.sh cdc_merge_stream
Debug on a laptop (Kafka + Postgres exposed on localhost):
    CDC_KAFKA_SERVERS=localhost:29093 python jobs/cdc_merge_stream.py --once --verbose
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import cdc, cli, config, schema, sparkutils  # noqa: E402
from common import cdc_merge_common as common  # noqa: E402

JOB = "cdc_merge_stream"


def ensure_targets(spark) -> None:
    """Create the Iceberg dimension tables (business columns + CDC bookkeeping)."""
    schema.ensure_namespaces(spark)
    from common import dimensions

    for ddl in dimensions.iceberg_ddl(config.ICEBERG_CATALOG).split("\n\n"):
        if ddl.strip():
            spark.sql(ddl)


def main(argv=None) -> int:
    parser = cli.build_parser(JOB, extra=[
        (("--mirror-postgres",), {"action": "store_true", "help": "also mirror dims to Postgres"}),
    ])
    args = parser.parse_args(argv)
    if cli.maybe_print_config(args):
        return 0
    cli.announce(JOB, args)

    spark = sparkutils.get_spark(JOB)
    ensure_targets(spark)
    stream = common.read_cdc(spark, streaming=not args.once, starting=args.starting,
                            max_offsets=args.max_rows_per_trigger)

    def sink(spark_, micro, batch_id):
        stats = common.apply_changes(spark_, micro, dry_run=args.dry_run)
        if stats.get("events"):
            # remember the Iceberg snapshot our own tables reached: lets the batch
            # DAG (and any other consumer) read "only what changed since last run"
            for table in (config.TABLE_MERCHANT_DIM, config.TABLE_CARD_DIM):
                cdc.record_snapshot(spark_, table, cdc.current_snapshot_id(spark_, table))
            if args.mirror_postgres:
                common.publish_dimensions_to_serving(spark_)
        print(f"[{JOB}] {stats}", flush=True)

    if args.once:
        bounded = common.read_cdc(spark, streaming=False, starting="earliest")
        print(common.apply_changes(spark, bounded, dry_run=args.dry_run), flush=True)
        return 0

    sparkutils.start_query(stream, name=JOB, checkpoint=common.checkpoint_for(JOB), sink=sink)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
