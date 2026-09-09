"""CDC = Change Data Capture: stream *row-level changes* of the dimension
tables (Postgres) into the lakehouse instead of re-copying them every night.

Wire format is Debezium's: every event carries ``op`` (c/u/d/r), ``before``,
``after``, ``ts_ms`` and the source table.  We flatten it into a "change view"
and let Iceberg ``MERGE INTO`` apply it — an ACID upsert *and* delete, which a
plain "append parquet to S3" pipeline cannot do.

Three ways to consume the same stream (all implemented here):
  1. streaming  : Kafka topic  -> MERGE INTO iceberg        (jobs/cdc_merge_stream.py)
  2. batch/once : Kafka topic (maxOffsetPerTrigger) -> MERGE (jobs/cdc_merge_batch.py)
  3. lake-side  : read another Iceberg table with
                  `incremental` + startSnapshotId            (see read_incremental)
"""
from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from . import config

INSERT, UPDATE, DELETE, SNAPSHOT_READ = "INSERT", "UPDATE", "DELETE", "READ"
STATE_TABLE = config.TABLE_SNAPSHOT_STATE

#: columns every CDC-managed Iceberg dimension table carries
MANAGED_COLUMNS = ("op", "source_db", "source_table", "source_ts", "source_ts_ms", "ingest_ts")


# ------------------------------------------------------------------- parsing
def parse_cdc_stream(df: DataFrame) -> DataFrame:
    """Kafka rows with a Debezium envelope (schema-wrapped or not) -> flat changes."""
    value = F.col("value").cast("string")
    # `{"schema":..,"payload":..}` when schemas.enable=true, otherwise the bare record.
    body = F.coalesce(F.get_json_object(value, "$.payload"), value)

    return (
        df.withColumn("body", body)
          .withColumn("op_raw", F.get_json_object("body", "$.op"))
          .withColumn("ts_ms", F.get_json_object("body", "$.ts_ms").cast("long"))
          .withColumn("source_table", F.get_json_object("body", "$.source.table"))
          .withColumn("source_schema", F.get_json_object("body", "$.source.schema"))
          .withColumn("source_db", F.get_json_object("body", "$.source.db"))
          .withColumn("source_id", F.get_json_object("body", "$.source.id"))
          .withColumn("lsn", F.get_json_object("body", "$.source.lsn"))
          .withColumn("before_json", F.get_json_object("body", "$.before"))
          .withColumn("after_json", F.get_json_object("body", "$.after"))
          .withColumn(
              "op",
              F.when(F.col("op_raw") == "c", F.lit(INSERT))
               .when(F.col("op_raw") == "u", F.lit(UPDATE))
               .when(F.col("op_raw") == "d", F.lit(DELETE))
               .when(F.col("op_raw") == "r", F.lit(SNAPSHOT_READ))
               .otherwise(F.lit(UPDATE)),
          )
          .withColumn("source_ts_ms",
                      F.coalesce(F.col("ts_ms"), (F.col("timestamp").cast("long") * 1000)))
          .withColumn("source_ts", (F.col("source_ts_ms") / 1000).cast("timestamp"))
          .withColumn("is_delete", F.col("op") == F.lit(DELETE))
          .withColumn("ingest_ts", F.current_timestamp())
          .drop("op_raw", "ts_ms")
    )


def change_columns_for(table: str) -> list[str]:
    """Business columns we expect in the Debezium `after`/`before` JSON."""
    from . import dimensions

    return dimensions.DIMENSION_BUSINESS_COLUMNS[table]


def with_flat_columns(changes: DataFrame, columns: Sequence[str]) -> DataFrame:
    """Explode ``after_json``/``before_json`` into real columns.

    ``after_json`` is NULL for deletes, so the surviving ``before_json`` image
    tells us *which* row to delete.
    """
    image = F.coalesce(F.col("after_json"), F.col("before_json"))
    out = changes
    for col in columns:
        out = out.withColumn(col, F.get_json_object(image, f"$.{col}"))
    return out


def latest_change_per_key(changes: DataFrame, keys: Sequence[str]) -> DataFrame:
    """One event per entity per micro-batch: the newest (deletes win ties)."""
    w = Window.partitionBy(*keys).orderBy(F.col("source_ts_ms").desc(), F.col("op").desc())
    return (changes.withColumn("__rn", F.row_number().over(w))
                  .where(F.col("__rn") == 1).drop("__rn"))


# ---------------------------------------------------------------- merge logic
def target_data_columns(spark: SparkSession, table_fqn: str) -> list[str]:
    """Business columns of the target table (i.e. excluding our CDC bookkeeping)."""
    return [f.name for f in spark.table(table_fqn).schema.fields if f.name not in MANAGED_COLUMNS]


def merge_changes(
    spark: SparkSession,
    changes: DataFrame,
    target_table: str,
    keys: Sequence[str],
    source_view: str = "cdc_changes",
    allow_delete: bool = True,
) -> None:
    """Apply a change set to an Iceberg table with MERGE INTO.

    Only *newer* events (by ``source_ts_ms``) overwrite older ones, so replaying
    the topic or arriving out of order is harmless — the table is the
    last-known-good state of the source.
    """
    cols = target_data_columns(spark, target_table)
    for key in keys:
        if key not in cols:
            raise ValueError(f"merge key {key!r} is not a data column of {target_table}: {cols}")

    prepared = with_flat_columns(changes, cols)
    prepared = latest_change_per_key(prepared, keys)
    prepared.createOrReplaceTempView(source_view)

    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    update_set = ",\n       ".join(f"t.{c} = s.{c}" for c in cols)
    bookkeeping_update = ",\n       ".join(
        f"t.{c} = s.{c}" for c in ("source_ts", "source_ts_ms", "source_db", "source_table", "op")
    )
    insert_cols = ", ".join(cols + ["source_ts", "source_ts_ms", "source_db", "source_table", "op", "ingest_ts"])
    insert_vals = ", ".join(f"s.{c}" for c in cols + ["source_ts", "source_ts_ms", "source_db", "source_table", "op", "ingest_ts"])
    delete_clause = (
        "WHEN MATCHED AND s.op = 'DELETE' AND s.source_ts_ms >= coalesce(t.source_ts_ms, 0) THEN DELETE\n"
        if allow_delete else ""
    )

    spark.sql(f"""
MERGE INTO {target_table} t
USING {source_view} s
ON {on_clause}
{delete_clause}WHEN MATCHED AND s.op <> 'DELETE' AND s.source_ts_ms >= coalesce(t.source_ts_ms, 0) THEN
  UPDATE SET
       {update_set},
       {bookkeeping_update}
WHEN NOT MATCHED AND s.op <> 'DELETE' THEN
  INSERT ({insert_cols})
  VALUES ({insert_vals})
""")


#: source table -> (iceberg table, merge keys)
def dimension_targets() -> dict[str, tuple[str, list[str]]]:
    return {
        "merchants": (config.TABLE_MERCHANT_DIM, ["merchant_id"]),
        "card_accounts": (config.TABLE_CARD_DIM, ["card_id"]),
    }


def apply_changes_by_table(spark: SparkSession, changes: DataFrame) -> dict[str, int]:
    """Route a change DataFrame to each target table; returns rows applied."""
    counts: dict[str, int] = {}
    targets = dimension_targets()
    for source_table, (target, keys) in targets.items():
        subset = changes.where(F.col("source_table") == source_table)
        n = subset.count()
        if n:
            merge_changes(spark, subset, target, keys)
        counts[target] = n
    return counts


# ------------------------------------------------- incremental reads + state
def current_snapshot_id(spark: SparkSession, table: str) -> int | None:
    try:
        row = spark.table(f"{table}.snapshots").orderBy(F.col("committed_at").desc()).first()
    except Exception:  # table may not exist yet
        return None
    return None if row is None else int(row["snapshot_id"])


def _ensure_state_table(spark: SparkSession) -> None:
    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
  source_table string, last_snapshot_id bigint, last_commit_at timestamp, updated_at timestamp
) USING iceberg
""")


def last_applied_snapshot(spark: SparkSession, table: str) -> int | None:
    _ensure_state_table(spark)
    try:
        row = spark.sql(f"SELECT last_snapshot_id FROM {STATE_TABLE} WHERE source_table = '{table}'").first()
    except Exception:
        return None
    return None if row is None or row["last_snapshot_id"] is None else int(row["last_snapshot_id"])


def record_snapshot(spark: SparkSession, table: str, snapshot_id: int | None) -> None:
    if snapshot_id is None:
        return
    _ensure_state_table(spark)
    spark.sql(f"""
MERGE INTO {STATE_TABLE} t
USING (SELECT '{table}' AS source_table, {snapshot_id} AS last_snapshot_id,
              current_timestamp() AS last_commit_at, current_timestamp() AS updated_at) s
ON t.source_table = s.source_table
WHEN MATCHED THEN UPDATE SET t.last_snapshot_id = s.last_snapshot_id,
                              t.last_commit_at = s.last_commit_at,
                              t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT *
""")


def read_incremental(spark: SparkSession, table: str, start_snapshot_id: int | None,
                     end_snapshot_id: int | None = None) -> DataFrame:
    """Iceberg's answer to "everything that changed between two snapshots"."""
    reader = spark.read.format("iceberg")
    if start_snapshot_id:
        reader = reader.option("incremental", "true").option("startSnapshotId", int(start_snapshot_id))
        if end_snapshot_id:
            reader = reader.option("endSnapshotId", int(end_snapshot_id))
    return reader.table(table)


def read_appended_files_since(spark: SparkSession, table: str, minutes: int = 30) -> DataFrame:
    """Alternative 'since last run' read: only files *added* recently, then the
    table rows filtered to those file paths.  Useful when snapshot ids are not
    tracked.  Demonstrated in docs/03-iceberg.md."""
    return spark.sql(f"""
      SELECT * FROM {table}
      WHERE input_file_name() IN (
        SELECT file_path FROM {table}.files
        WHERE added_at_ts >= (unix_timestamp() - {int(minutes) * 60}) * 1000000
      )
    """)


# ------------------------------------------------------------- connector json
def debezium_connector_config(
    name: str = "haweye-dimensions",
    database: str = "dimensions",
    hostname: str = "postgres-cdc",
    port: int = 5432,
    user: str = "debezium",          # dedicated REPLICATION role, see cdc/sql
    password: str = "debezium",
    tables: Sequence[str] = ("public.merchants", "public.card_accounts"),
    publication: str = config.CDC_PUBLICATION,
    slot: str = "haweye_cdc_slot",
    emit_schema: bool = False,
) -> dict:
    """Body for `POST /connectors` (see cdc/register_connector.sh)."""
    return {
        "name": name,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": hostname,
            "database.port": str(port),
            "database.user": user,
            "database.password": password,
            "database.dbname": database,
            "topic.prefix": "cdc",
            "schema.include.list": "public",
            "table.include.list": ",".join(tables),
            "publication.name": publication,
            "slot.name": slot,
            "plugin.name": "pgoutput",
            "snapshot.mode": "initial",
            "heartbeat.interval.ms": "10000",
            # Both key and value are Structs for a relational table, so the JSON
            # converter is mandatory: `StringConverter` on the key dies with
            # "Converter ... does not handle map values" the first time a row is
            # captured.  (StringConverter keys are only correct for the Debezium
            # *outbox* pattern, where the key really is a string.)
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "true" if emit_schema else "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true" if emit_schema else "false",
            # numeric dimension columns must arrive as plain JSON numbers, not
            # base64-encoded Connect Decimals
            "decimal.handling.mode": "double",
            "bigint.unsafe.mode": "false",
            "errors.tolerance": "all",
            "errors.deadletterqueue.topic.name": f"{name}.dlq",
            "errors.deadletterqueue.context.headers.enable": "true",
            "poll.interval.ms": "500",
            "max.batch.size": "2048",
            "tombstones.on.delete": "false",
        },
    }
