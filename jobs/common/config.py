"""Single source of truth for every connection string / knob in the project.

Everything reads an environment variable first, so the *same* code runs in a
Docker container (compose injects the env), in Airflow (same), and on your
laptop (defaults point at localhost).
"""
from __future__ import annotations

import os


def _b(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _i(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _f(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


# --------------------------------------------------------------- object store
S3_BUCKET = os.environ.get("LAKEHOUSE_S3_BUCKET", os.environ.get("S3_BUCKET", "lakehouse"))
WAREHOUSE = os.environ.get("LAKEHOUSE_WAREHOUSE", f"s3a://{S3_BUCKET}/warehouse")
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
MINIO_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "http://minio:9000")
# endpoint *without* scheme, the form Iceberg's S3FileIO prefers
MINIO_ENDPOINT_NO_SCHEME = MINIO_ENDPOINT.split("://", 1)[-1]

# -------------------------------------------------------------------- iceberg
ICEBERG_CATALOG = os.environ.get("ICEBERG_CATALOG", "lake")
ICEBERG_NS_RAW = os.environ.get("ICEBERG_NS_RAW", "raw")
ICEBERG_NS_DIM = os.environ.get("ICEBERG_NS_DIM", "dim")
ICEBERG_NS_FEATURES = os.environ.get("ICEBERG_NS_FEATURES", "features")
ICEBERG_NS_MARTS = os.environ.get("ICEBERG_NS_MARTS", "marts")

TABLE_RAW = f"{ICEBERG_CATALOG}.{ICEBERG_NS_RAW}.transactions_raw"
TABLE_ENRICHED = f"{ICEBERG_CATALOG}.{ICEBERG_NS_RAW}.transactions_enriched"
TABLE_FEATURES = f"{ICEBERG_CATALOG}.{ICEBERG_NS_FEATURES}.transactions_feature_v1"
TABLE_LABELS = f"{ICEBERG_CATALOG}.{ICEBERG_NS_RAW}.fraud_labels"
TABLE_MERCHANT_DIM = f"{ICEBERG_CATALOG}.{ICEBERG_NS_DIM}.merchants"
TABLE_CARD_DIM = f"{ICEBERG_CATALOG}.{ICEBERG_NS_DIM}.card_accounts"
TABLE_LOAD_FAILURES = f"{ICEBERG_CATALOG}.{ICEBERG_NS_RAW}.load_failures"
TABLE_SNAPSHOT_STATE = f"{ICEBERG_CATALOG}.{ICEBERG_NS_RAW}.cdc_snapshot_state"

# ------------------------------------------------------------------- postgres
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = _i("POSTGRES_PORT", 5432)
POSTGRES_USER = os.environ.get("POSTGRES_USER", "haweye")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "haweye")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "lakehouse")
CATALOG_DB = os.environ.get("ICEBERG_CATALOG_DB", "catalog")
CATALOG_SCHEMA = os.environ.get("ICEBERG_CATALOG_SCHEMA", "iceberg_catalog")
POSTGRES_DRIVER = os.environ.get("POSTGRES_DRIVER", "org.postgresql.Driver")

CATALOG_JDBC_URI = os.environ.get(
    "ICEBERG_CATALOG_URI",
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{CATALOG_DB}",
)


def pg_dsn(database: str | None = None, user: str | None = None, password: str | None = None) -> str:
    """Plain `postgresql://` URI, used by pandas/JDBC/psycopg2 (not by Iceberg)."""
    return (
        f"postgresql://{user or POSTGRES_USER}:{password or POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{database or POSTGRES_DB}"
    )


SERVING_DB_URI = os.environ.get(
    "SERVING_DB_URI",
    pg_dsn(POSTGRES_DB, os.environ.get("SERVING_DB_USER", POSTGRES_USER),
            os.environ.get("SERVING_DB_PASSWORD", POSTGRES_PASSWORD)),
)

# ---------------------------------------------------------------------- kafka
KAFKA_SERVERS = os.environ.get("KAFKA_SERVERS", "kafka:29092")
KAFKA_TOPIC_RAW = os.environ.get("KAFKA_TOPIC_RAW", "raw_transactions")
KAFKA_TOPIC_FEATURES = os.environ.get("KAFKA_TOPIC_FEATURES", "transactions_features")

# ---------------------------------------------------------------------- redis
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = _i("REDIS_PORT", 6379)
REDIS_URL = os.environ.get("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/0")
REDIS_FEATURE_PREFIX = os.environ.get("REDIS_FEATURE_PREFIX", "feat:txn:")
REDIS_SCORE_PREFIX = os.environ.get("REDIS_SCORE_PREFIX", "score:txn:")
REDIS_CARD_PREFIX = os.environ.get("REDIS_CARD_PREFIX", "feat:card:")
REDIS_TTL_SECONDS = _i("REDIS_TTL_SECONDS", 86_400)

# --------------------------------------------------------------------- streaming
STREAM_TRIGGER_SECONDS = _i("STREAM_TRIGGER_SECONDS", 10)
LOOKBACK_MINUTES = _i("FEATURE_LOOKBACK_MINUTES", 60)
WATERMARK_HOURS = _f("STREAM_WATERMARK_HOURS", 2.0)
MAX_ROWS_PER_TRIGGER = _i("STREAM_MAX_ROWS_PER_TRIGGER", 20_000)

# ------------------------------------------------------------------- model/serving
MODEL_URI = os.environ.get("FRAUD_MODEL_URI", f"s3a://{S3_BUCKET}/models/fraud_rf")
MODEL_POINTER_FILE = os.environ.get("FRAUD_MODEL_POINTER", "version.txt")
SCORE_THRESHOLD = _f("FRAUD_SCORE_THRESHOLD", 0.6)
ALERT_MIN_SCORE = _f("ALERT_MIN_SCORE", 0.75)
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "").strip() or None

# --------------------------------------------------------------------------- cdc
CDC_ENABLED = _b("CDC_ENABLED", False)
CDC_KAFKA_SERVERS = os.environ.get("CDC_KAFKA_SERVERS", "kafka-connect-kafka:29092")
CDC_TOPIC_PREFIX = os.environ.get("CDC_TOPIC_PREFIX", "cdc.public")
CDC_PUBLICATION = os.environ.get("CDC_PUBLICATION", "haweye_cdc")
CDC_PG_HOST = os.environ.get("CDC_PG_HOST", "postgres-cdc")
CDC_PG_PORT = _i("CDC_PG_PORT", 5432)
CDC_PG_DB = os.environ.get("CDC_PG_DB", "dimensions")
CDC_PG_USER = os.environ.get("CDC_PG_USER", "cdc")
CDC_PG_PASSWORD = os.environ.get("CDC_PG_PASSWORD", "cdc")
CDC_TABLES = ("merchants", "card_accounts")


def cdc_topic(table: str) -> str:
    """Debezium publishes one topic per captured table: `<prefix>.<schema>.<table>`."""
    return f"{CDC_TOPIC_PREFIX}.{table}"


def describe() -> dict[str, object]:
    """Used by `--print-config` in every job so ops can see what a job will use."""
    return {
        "warehouse": WAREHOUSE,
        "iceberg_catalog_jdbc": CATALOG_JDBC_URI,
        "tables": {
            "raw": TABLE_RAW,
            "enriched": TABLE_ENRICHED,
            "features": TABLE_FEATURES,
            "labels": TABLE_LABELS,
            "merchant_dim": TABLE_MERCHANT_DIM,
            "card_dim": TABLE_CARD_DIM,
        },
        "kafka": {"bootstrap": KAFKA_SERVERS, "raw_topic": KAFKA_TOPIC_RAW},
        "serving_db": SERVING_DB_URI.split("@")[-1],
        "redis": REDIS_URL,
        "model_uri": MODEL_URI,
        "score_threshold": SCORE_THRESHOLD,
        "streaming": {
            "trigger_seconds": STREAM_TRIGGER_SECONDS,
            "lookback_minutes": LOOKBACK_MINUTES,
            "watermark_hours": WATERMARK_HOURS,
        },
        "cdc": {
            "enabled": CDC_ENABLED,
            "bootstrap": CDC_KAFKA_SERVERS,
            "topics": [cdc_topic(t) for t in CDC_TABLES],
        },
    }
