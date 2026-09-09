"""The transaction contract: schema, parsing, validation, quality gates.

Why a hand-written schema instead of ``inferSchema``?  A producer that adds or
renames a field must never silently change the shape of your tables.  The
schema below is explicit, unknown extra fields are tolerated (``PERMISSIVE``
mode), and anything that fails validation lands in a *quarantine* Iceberg
table instead of killing the pipeline.
"""
from __future__ import annotations

import json
import os
from functools import reduce
from operator import and_, or_

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

# --------------------------------------------------------------------- schema
TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("event_ts", StringType(), False),          # ISO-8601 text, parsed below
        StructField("card_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("channel", StringType(), True),            # online | pos | atm
        StructField("merchant_country", StringType(), True),
        StructField("card_present", BooleanType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("is_3ds", BooleanType(), True),
        StructField("device_id", StringType(), True),
        StructField("raw_json", StringType(), True),            # audit trail: original payload
        StructField("_corrupt_record", StringType(), True),
    ]
)
TYPED_FIELDS = [f.name for f in TRANSACTION_SCHEMA.fields
                if f.name not in ("raw_json", "_corrupt_record")]

REQUIRED_FIELDS = ("transaction_id", "event_ts", "card_id", "merchant_id", "amount")
CHANNELS = ("online", "pos", "atm")
#: Countries the contract accepts.  Keep this a superset of
#: `generator/simulator.py:HIGH_RISK_COUNTRIES` - test_generator_contract asserts it,
#: because a producer that emits a code the consumer quarantines is the quietest
#: possible bug (rows vanish into `load_failures` and nothing fails).
COUNTRIES = (
    "US", "GB", "DE", "FR", "ES", "IT", "NL", "PL", "CA", "BR", "MX", "IN",
    "SG", "JP", "AU", "AE", "ZA", "TR", "SE", "NO", "CN", "KR", "AR", "CO", "NG",
    "VN", "PK", "BD", "RU", "UA",
)
DEDUP_KEY = "dedup_key"
# Reject nonsense timestamps, never late data: "late" is the watermark's job
# (`docs/04-streaming.md`), and a gate that confuses the two quarantines a replay.
# Configurable because replaying old Kafka retention is a documented operation
# here -- with the default, anything older than this lands in raw.load_failures.
STALE_GUARD_DAYS = int(os.environ.get("SCHEMA_STALE_GUARD_DAYS", "400"))


def transaction_schema() -> StructType:
    return TRANSACTION_SCHEMA


def _to_timestamp(col: Column) -> Column:
    """Accept ``2024-06-01T10:15:30Z``, ``...+02:00`` and ``2024-06-01 10:15:30``."""
    c = F.col(col) if isinstance(col, str) else col
    normalised = F.regexp_replace(
        F.regexp_replace(c.cast("string"), "T", " "),
        r"(\.\d+)?(Z|[+-]\d{2}:?\d{2})$",
        "",
    )
    return F.to_timestamp(normalised, "yyyy-MM-dd HH:mm:ss")


# ------------------------------------------------------------------- parsing
def parse_transactions(df: DataFrame) -> DataFrame:
    """Kafka rows (key/value/topic/partition/offset) -> typed transaction rows."""
    parsed = (
        df.withColumn("payload_json", F.col("value").cast("string"))
          .withColumn("parsed", F.from_json(F.col("payload_json"), TRANSACTION_SCHEMA))
    )
    return parsed.select(
        # `payload_json` is carried on purpose, and the two consumers below are why:
        # it is the dedupe fallback for a payload with no transaction_id, and
        # `quarantine_rows` writes it to `raw.load_failures` so a rejected event can
        # be replayed verbatim.  Consumers that do not want it drop it explicitly
        # (`streaming_ingestion.process_batch` does) - the lakehouse tables have no
        # such column, and the projection here is what decides that.
        F.col("payload_json"),
        *[F.col(f"parsed.{name}").alias(name) for name in TYPED_FIELDS],
        F.col("parsed.raw_json"),
        F.col("parsed._corrupt_record"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.col("timestamp").alias("kafka_ts"),
    ).withColumn("event_ts_ts", _to_timestamp("event_ts")) \
     .withColumn(
         DEDUP_KEY,
         F.md5(F.concat_ws("|", F.coalesce(F.col("transaction_id"), F.col("payload_json")))),
     ) \
     .withColumn("ingest_ts", F.current_timestamp()) \
     .withColumn("dt", F.to_date(F.col("event_ts_ts")))


def dedupe(df: DataFrame, key: str = DEDUP_KEY) -> DataFrame:
    """Drop duplicates *inside* the micro-batch, keeping the newest Kafka offset.

    Cross-micro-batch duplicates are handled idempotently at write time
    (``MERGE ... WHEN NOT MATCHED``), so a Kafka replay cannot double-count.
    """
    w = Window.partitionBy(key).orderBy(F.col("source_offset").desc())
    return (df.withColumn("__rn", F.row_number().over(w))
              .where(F.col("__rn") == 1)
              .drop("__rn"))


# -------------------------------------------------------------- quality gates
def quality_checks() -> dict[str, Column]:
    """Named boolean predicates; ``True`` means the row is *fine*.

    The allow-lists go through ``sparkutils.isin`` rather than ``Column.isin``: they
    are tuples, and pyspark reads a tuple as a single literal value, so the naive
    form fails analysis on every micro-batch with
    ``UNSUPPORTED_FEATURE.LITERAL_TYPE``.  (Local import: ``sparkutils`` is the
    session/lifecycle adapter and this module is imported by tests that never touch a
    JVM, so the dependency stays optional at import time -- same reason ``config`` is
    imported locally below.)
    """
    from . import sparkutils
    _isin = sparkutils.isin
    return {
        "has_required_fields": reduce(and_, [F.col(n).isNotNull() for n in REQUIRED_FIELDS]),
        "amount_positive": F.col("amount") > 0,
        "amount_sane": F.col("amount") < 1_000_000,
        "event_time_parseable": F.col("event_ts_ts").isNotNull(),
        "event_time_not_in_future": F.col("event_ts_ts") <= F.current_timestamp() + F.expr("interval 15 minutes"),
        "event_time_not_stale": F.col("event_ts_ts") >= F.current_timestamp() - F.expr(f"interval {STALE_GUARD_DAYS} days"),
        "currency_ok": F.length(F.coalesce(F.col("currency"), F.lit("USD"))) == 3,
        "channel_ok": _isin("channel", CHANNELS) | F.col("channel").isNull(),
        "country_ok": _isin("merchant_country", COUNTRIES) | F.col("merchant_country").isNull(),
    }


def add_quality_flags(df: DataFrame) -> DataFrame:
    """Attach one ``qc_<check>`` boolean per rule plus the aggregate verdict.

    ``F.coalesce(pred, lit(False))`` and not ``pred.fillna(False)``: ``Column`` has no
    ``fillna``, and its ``__getattr__`` happily returns *another column* named
    ``fillna`` -- so the typo only shows up as ``TypeError: 'Column' object is not
    callable`` at the call, and would otherwise have been a silent "every row fails"
    if it had resolved.  NULL means "this check could not be evaluated" (a missing
    field on a malformed event), which is a failure, not a pass: coalescing to False
    *before* the negation is what makes an unparseable row land in quarantine.
    """
    checks = quality_checks()
    names = list(checks.keys())
    for name in names:
        df = df.withColumn(f"qc_{name}", ~F.coalesce(checks[name], F.lit(False)))
    return df.withColumn("qc_failed", reduce(or_, [F.col(f"qc_{n}") for n in names])) \
             .withColumn("qc_reasons", F.array_compact(
                 F.array(*[F.when(F.col(f"qc_{n}"), F.lit(n)) for n in names])))


def split_good_and_bad(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    flagged = add_quality_flags(df)
    return (flagged.where(~F.col("qc_failed")), flagged.where(F.col("qc_failed")))


def quarantine_rows(bad: DataFrame) -> DataFrame:
    """Shape the quarantine DataFrame for the ``raw.load_failures`` table."""
    return bad.select(
        F.col("transaction_id"),
        F.col("payload_json"),
        F.col("qc_reasons").alias("failure_reasons"),
        F.col("source_topic"),
        F.col("source_partition"),
        F.col("source_offset"),
        F.col("kafka_ts"),
        F.col("ingest_ts"),
    )


def quarantine_schema() -> StructType:
    return StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("payload_json", StringType(), True),
            StructField("failure_reasons", ArrayType(StringType()), True),
            StructField("source_topic", StringType(), True),
            StructField("source_partition", LongType(), True),
            StructField("source_offset", LongType(), True),
            StructField("kafka_ts", TimestampType(), True),
            StructField("ingest_ts", TimestampType(), True),
        ]
    )


# ------------------------------------------- pure-python validation (no Spark)
def is_valid_payload(payload: dict | str) -> tuple[bool, list[str]]:
    """Used by the generator (self-check) and by unit tests."""
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            return False, [f"invalid_json:{exc.msg}"]
    problems: list[str] = []
    for field in REQUIRED_FIELDS:
        if payload.get(field) in (None, ""):
            problems.append(f"missing:{field}")
    amount = payload.get("amount")
    if amount is not None:
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            problems.append("amount_not_numeric")
        elif float(amount) <= 0:
            problems.append("amount_not_positive")
    if payload.get("channel") and payload["channel"] not in CHANNELS:
        problems.append("bad_channel")
    if payload.get("merchant_country") and payload["merchant_country"] not in COUNTRIES:
        problems.append("bad_country")
    return (not problems), problems


def ensure_namespaces(spark: SparkSession) -> None:
    """Iceberg namespaces (databases) are cheap and idempotent."""
    from . import config

    for ns in (config.ICEBERG_NS_RAW, config.ICEBERG_NS_DIM,
               config.ICEBERG_NS_FEATURES, config.ICEBERG_NS_MARTS):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.ICEBERG_CATALOG}.{ns}")
