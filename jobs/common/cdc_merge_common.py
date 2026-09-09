"""Shared plumbing for the two CDC jobs (streaming and one-shot batch).

What the CDC path does, in one paragraph: Debezium reads Postgres' write-ahead
log, Kafka Connect publishes one topic per table, and we apply those changes to
the Iceberg dimension tables with `MERGE INTO` (upsert **and** delete).  The
feature store then joins against tables that are minutes old instead of a day
old — and because Iceberg keeps every snapshot, the exact dimension state used
for a given transaction stays queryable forever.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import cdc, config


def read_cdc(spark, *, streaming: bool, starting: str = "latest",
             max_offsets: int | None = None) -> DataFrame:
    """Kafka rows from the Debezium topics (one per captured table)."""
    topics = ",".join(config.cdc_topic(t) for t in config.CDC_TABLES)
    reader = (spark.readStream if streaming else spark.read).format("kafka")
    reader = reader.option("kafka.bootstrap.servers", config.CDC_KAFKA_SERVERS)
    reader = reader.option("subscribe", topics)
    reader = reader.option("startingOffsets", "earliest" if starting in {"earliest", ""} else starting)
    reader = reader.option("failOnDataLoss", "false")
    if streaming and max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", str(max_offsets))
    if not streaming:
        reader = reader.option("endingOffsets", "latest" if starting == "latest" else starting)
    return reader.load()


def apply_changes(spark, micro: DataFrame, *, dry_run: bool = False) -> dict:
    """Normalise + route a change set into each Iceberg dimension table."""
    if micro.rdd.isEmpty():
        return {"events": 0}
    changes = cdc.parse_cdc_stream(micro)
    known = list(cdc.dimension_targets().keys())
    changes = changes.where(F.col("source_table").isin(known))
    n = changes.count()
    if not n or dry_run:
        return {"events": n, "dry_run": dry_run}
    counts = cdc.apply_changes_by_table(spark, changes)
    return {"events": n, "applied": counts}


def publish_dimensions_to_serving(spark) -> int:
    """Mirror the merged dimensions into Postgres so the alert console / API can
    join them without a Spark session (the 'lakehouse -> app' direction)."""
    from common import dimensions, sparkutils
    from common import io as hio

    written = 0
    for table, spec in dimensions.TABLES.items():
        fqn = f"{config.ICEBERG_CATALOG}.dim.{table}"
        if not sparkutils.table_exists(spark, fqn):
            continue
        cols = [f.name for f in spark.table(fqn).schema.fields
                if f.name not in ("op", "source_db", "source_table", "ingest_ts", "source_ts_ms")]
        rows = spark.table(fqn).select(*cols).collect()
        payload = [tuple(_clean(getattr(r, c)) if hasattr(r, c) else r[c] for c in cols) for r in rows]
        out_cols = [c for c in cols if c in hio.dimension_serving_columns(table)]
        if not out_cols:
            continue
        mapped = [tuple(p[cols.index(c)] for c in out_cols) for p in payload]
        written += hio.upsert_rows(f"public.{table}_lakehouse",
                                    out_cols + ["synced_at"], [spec["pk"]],
                                    [m + (_now(),) for m in mapped],
                                    update_columns=[c for c in out_cols if c != spec["pk"]])
    return written


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(sep=" ", timespec="seconds")


def _clean(v):
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat(sep=" ", timespec="seconds") if hasattr(v, "timetz") else str(v)
    return v


def checkpoint_for(name: str) -> str:
    return f"{config.WAREHOUSE}/checkpoints/{name}"
