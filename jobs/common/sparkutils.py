"""Everything Spark-related that is not business logic: session, catalogs,
streaming lifecycle, idempotent writes.  Written once, used by all jobs, so a
job file stays readable as a description of *what* it does.
"""
from __future__ import annotations

import os
import signal
import threading
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from . import config

ICEBERG_RUNTIME = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{os.environ.get('ICEBERG_VERSION', '1.5.2')}"


# --------------------------------------------------------------------- session
def get_spark(app_name: str, *, local: bool | None = None, extra_conf: dict | None = None) -> SparkSession:
    """Build a SparkSession wired to Iceberg + MinIO + the Postgres catalog.

    Works unchanged in three places:
      * inside the cluster (compose already has the jars + spark-defaults.conf)
      * Airflow (`spark-submit` into the same cluster)
      * on your laptop in local mode (`SPARK_MASTER=local[*]` + `--jars`)
    """
    builder = SparkSession.builder.appName(app_name)
    master = os.environ.get("SPARK_MASTER")
    if local or (master and master.startswith("local")):
        builder = builder.master(master or "local[2]")
        for k, v in local_iceberg_conf().items():
            builder = builder.config(k, v)
    else:
        if master:
            builder = builder.master(master)

    builder = builder.config("spark.sql.extensions",
                            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    for k, v in catalog_conf().items():
        builder = builder.config(k, v)
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def catalog_conf() -> dict[str, str]:
    """Iceberg catalog: metadata in Postgres, data on MinIO, S3FileIO for reads/writes."""
    c = config
    return {
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}.type": "jdbc",
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}.uri": c.CATALOG_JDBC_URI,
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}.jdbc.user": c.POSTGRES_USER,
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}.jdbc.password": c.POSTGRES_PASSWORD,
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}.warehouse": c.WAREHOUSE,
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}.jdbc.schema-version": "1",
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}.s3.endpoint": c.MINIO_ENDPOINT,
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}.s3.path-style-access": "true",
        f"spark.sql.catalog.{c.ICEBERG_CATALOG}.client.factory": "software.amazon.awssdk.enhanced.s3.S3SdkClientFactory",
        "spark.sql.legacy.createHiveTableByDefault": "false",
        # plain `spark.read.parquet("s3a://...")`, `df.write.parquet(...)` (models!)
        "spark.hadoop.fs.s3a.access.key": c.AWS_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": c.AWS_SECRET_KEY,
        "spark.hadoop.fs.s3a.endpoint": c.MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.s3a.impl": "org.apache.hadoop.fs.s3a.S3A",
        # Iceberg S3FileIO also needs credentials when they are not on the env
        "spark.hadoop.fs.s3a.fast.upload": "true",
    }


def local_iceberg_conf() -> dict[str, str]:
    """Extra toggles that make local mode pleasant."""
    return {
        "spark.sql.shuffle.partitions": os.environ.get("SHUFFLE_PARTITIONS", "4"),
        "spark.sql.streaming.ui.enabled": "false",
    }


# ------------------------------------------------------------------- streaming
def kafka_source(spark: SparkSession, topic: str | None = None, starting: str = "latest",
                 max_offsets: int | None = None, servers: str | None = None) -> DataFrame:
    """Unbounded DataFrame reading Kafka.  `starting`: latest | earliest | json offset."""
    opts = {
        "kafka.bootstrap.servers": servers or config.KAFKA_SERVERS,
        "subscribe": topic or config.KAFKA_TOPIC_RAW,
        "failOnDataLoss": "false",
    }
    if starting == "earliest":
        opts["startingOffsets"] = "earliest"
    elif starting == "latest":
        opts["startingOffsets"] = "latest"
    elif starting.startswith("{"):
        opts["startingOffsets"] = starting
    if max_offsets:
        opts["maxOffsetsPerTrigger"] = str(max_offsets)
    return spark.readStream.format("kafka").options(**opts).load()


def iceberg_source(spark: SparkSession, table: str, starting: str = "latest",
                   max_rows_per_trigger: int = 0) -> DataFrame:
    """Stream the *newly appended rows* of an Iceberg table (Iceberg 1.5 options).

    Semantics worth knowing before you use this (they are documented in
    docs/04-streaming.md#reading-iceberg-as-a-stream):

    * the source only understands **append** snapshots;
    * `streaming-skip-overwrite-snapshots` / `-delete-snapshots` are ON so that a
      concurrent `MERGE`/backfill on the same table cannot kill a 24/7 job;
    * `starting="latest"` resolves to the current snapshot id, i.e. "everything
      committed after now" - a restart continues from the checkpoint anyway.
    """
    reader = spark.readStream.format("iceberg")
    reader = (reader
              .option("streaming-skip-overwrite-snapshots", "true")
              .option("streaming-skip-delete-snapshots", "true"))
    if starting in ("latest", ""):
        current = current_snapshot_id(spark, table)
        if current is not None:
            reader = reader.option("stream-from-snapshot-id", current)
    elif starting.startswith("snapshot:"):
        reader = reader.option("stream-from-snapshot-id", int(starting.split(":", 1)[1]))
    elif starting.startswith("timestamp:"):
        reader = reader.option("stream-from-timestamp", str(_ts_ms(starting.split(":", 1)[1])))
    elif starting not in ("earliest",):
        raise ValueError(f"unknown --starting {starting!r}; use latest|earliest|snapshot:ID|timestamp:ISO")
    if max_rows_per_trigger:
        reader = reader.option("streaming-max-rows-per-micro-batch", str(int(max_rows_per_trigger)))
    return reader.table(table)


def current_snapshot_id(spark: SparkSession, table: str) -> int | None:
    """The snapshot a fresh stream should start *after* (None when the table is new)."""
    if not table_exists(spark, table):
        return None
    try:
        row = spark.sql(f"SELECT id AS s FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    except Exception:
        return None
    return int(row["s"]) if row and row["s"] is not None else None


def _first_snapshot(spark: SparkSession, table: str) -> int:
    row = spark.sql(f"SELECT min(snapshot_id) AS s FROM {table}.snapshots").first()
    return int(row["s"]) if row and row["s"] is not None else 0


def _ts_ms(value: str) -> int:
    from datetime import datetime, timezone

    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def trigger(*, once: bool = False) -> dict:
    """The one place that decides how often micro-batches run.

    Keyword args, not a `Trigger` object: `DataStreamWriter.trigger` is
    keyword-only in Spark 3.5 (`Trigger` is no longer importable from
    `pyspark.sql.streaming`), so this is the portable form.
    """
    if once:
        return {"once": True}
    return {"processingTime": f"{config.STREAM_TRIGGER_SECONDS} seconds"}


def start_query(df: DataFrame, name: str, checkpoint: str, output_mode: str = "append",
                sink: Callable | None = None, options: dict | None = None,
                await_termination: bool = True, once: bool = False) -> object:
    """Start (or resume) a named streaming query with a graceful stop handler.

    * `sink` receives `(spark, micro_batch_df, batch_id)` -> writes to Iceberg /
      Postgres / Redis.  Using `foreachBatch` is what makes the write *atomic*
      (one Iceberg snapshot per micro-batch) and lets us write to several
      destinations from one query.
    """
    writer = df.writeStream.queryName(name).option("checkpointLocation", checkpoint)
    if sink is not None:
        writer = writer.foreachBatch(lambda micro, bid: sink(spark_of(df), micro, bid))
    for k, v in (options or {}).items():
        writer = writer.option(k, v)
    query = writer.outputMode(output_mode).trigger(**trigger(once=once)).start()
    install_graceful_stop(query)
    if await_termination:
        query.awaitTermination()
    return query


def spark_of(df: DataFrame) -> SparkSession:
    return df.sparkSession


def install_graceful_stop(query) -> None:
    """SIGTERM/SIGINT -> `stop(False, True)`: finish the in-flight micro-batch.

    Without this, killing a streaming job mid-batch leaves an incomplete Kafka
    offset checkpoint and the next start replays (which is safe here because the
    writes are idempotent, but wasteful).
    """
    state = {"stopping": False}

    def _stop(signum, _frame):
        if state["stopping"]:
            return
        state["stopping"] = True
        print(f">>> got signal {signum}; finishing current micro-batch then stopping", flush=True)
        try:
            query.stop()
        except Exception as exc:  # pragma: no cover
            print(f"!!! graceful stop failed: {exc}", flush=True)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _stop)
        except ValueError:
            # not the main thread (e.g. running inside a test) - ignore
            pass
    threading.main_thread()  # keeps import side effects obvious


# ---------------------------------------------------------------------- writes
def table_exists(spark: SparkSession, table: str) -> bool:
    """Simple, reliable, catalog-agnostic: can we resolve the table?"""
    try:
        spark.table(table).printSchema(numLines=0)
        return True
    except Exception:
        return False


def create_table_from(df: DataFrame, table: str, *, partition_by: list[str] | None,
                      props: dict, mode: str = "append") -> None:
    """Create an Iceberg table from a DataFrame's *schema* (empty is fine)."""
    writer = df.write.format("iceberg")
    for k, v in props.items():
        writer = writer.option(k, v)
    if partition_by:
        writer = writer.partitionedBy(*partition_by)
    if table_exists(df.sparkSession, table):
        return
    writer.mode(mode).saveAsTable(table)


def ensure_table(spark: SparkSession, table: str, schema, *, partition_by: list[str] | None = None,
                 props: dict | None = None) -> None:

    if table_exists(spark, table):
        return
    empty = spark.createDataFrame([], schema)
    writer = empty.write.format("iceberg")
    for k, v in (props or {}).items():
        writer = writer.option(k, v)
    if partition_by:
        writer = writer.partitionedBy(*partition_by)
    writer.saveAsTable(table)


def append_batch(spark: SparkSession, table: str, df: DataFrame, *, props: dict | None = None,
                 partition_by: list[str] | None = None, create_if_missing: bool = True) -> None:
    """Idempotency comes from the checkpoint, not from MERGE: safe for append-only
    tables (raw records) where a duplicate would be corrected downstream."""
    writer = df.write.format("iceberg").mode("append")
    for k, v in (props or {}).items():
        writer = writer.option(k, v)
    if create_if_missing and not table_exists(spark, table):
        if partition_by:
            writer.partitionedBy(*partition_by).saveAsTable(table)
        else:
            writer.saveAsTable(table)
        return
    writer.saveAsTable(table)


def merge_batch(spark: SparkSession, table: str, df: DataFrame, key_cols: list[str],
                *, update_cols: list[str] | None = None, allow_update: bool = False) -> None:
    """`MERGE INTO ... WHEN NOT MATCHED THEN INSERT` = exactly-once *content*.

    Re-running a micro-batch (Kafka replay, checkpoint loss, backfill overlap)
    therefore never double-counts — the key is the dedup contract.  Set
    `allow_update=True` to also refresh changed values.
    """
    if not table_exists(spark, table):
        raise RuntimeError(f"{table} does not exist yet; create it with ensure_table()")
    view = f"_merge_input_{abs(hash(table)) % 100000}"
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
    if allow_update and update_cols:
        upd = ", ".join(f"t.{c} = s.{c}" for c in update_cols)
        clause = f"WHEN MATCHED THEN UPDATE SET {upd}"
    else:
        clause = ""
    spark.sql(f"""
MERGE INTO {table} t
USING {view} s
ON {on}
{clause}
WHEN NOT MATCHED THEN INSERT *
""")
    spark.catalog.dropTempView(view)


def write_scalar_file(spark: SparkSession, path: str, values: dict) -> None:
    """Overwrite a single-file marker (used for the model `version.txt` pointer).

    Object stores have no atomic rename for the general case, but "one csv part
    file, overwrite mode" is how Spark itself signals readiness and is fine for
    a pointer that is re-read on every deploy.
    """
    df = spark.createDataFrame([tuple(values.values())], list(values.keys()))
    df.coalesce(1).write.mode("overwrite").option("header", "false").csv(path)


def snapshot_summary(spark: SparkSession, table: str, limit: int = 5) -> list[dict]:
    try:
        rows = spark.sql(
            f"SELECT snapshot_id, parent_id, operation, summary, committed_at "
            f"FROM (SELECT * FROM {table}.snapshots ORDER BY committed_at DESC LIMIT {int(limit)})"
        ).collect()
    except Exception as exc:
        return [{"error": str(exc)}]
    out = []
    for r in rows:
        d = r.asDict()
        d["summary"] = dict(d["summary"]) if d.get("summary") else {}
        out.append(d)
    return out


def print_table_state(spark: SparkSession, table: str) -> None:
    """One-glance debug output that has saved me hours. Used by every job's CLI."""
    if not table_exists(spark, table):
        print(f"table {table}: <missing>")
        return
    n = spark.table(table).count()
    print(f"table {table}: {n} rows")
    for s in snapshot_summary(spark, table, limit=3):
        print("   ", s.get("snapshot_id"), s.get("operation"), s.get("committed_at"),
              {k: v for k, v in (s.get("summary") or {}).items() if k.startswith("added")})
