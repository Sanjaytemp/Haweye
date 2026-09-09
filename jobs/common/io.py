"""Sinks and sources outside the lakehouse: Postgres (serving tables, dimension
master) and Redis (the *online* feature store).

Rule of thumb used throughout this repo:
    Iceberg  = the system of record (batch, replay, ML training, audit)
    Postgres = the operational store the app/alert console reads
    Redis    = the low-latency lookup the scorer/feature-API reads
Same numbers, three shapes.  The jobs write all three in the same micro-batch,
so they can never disagree for more than one trigger interval.
"""
from __future__ import annotations

import contextlib
import json
import math
import os
import re
from collections.abc import Iterable, Sequence
from datetime import date, datetime
from typing import Any

import psycopg2
import psycopg2.extras
from psycopg2 import sql as psql

from . import config

SQL_DIR = os.environ.get("SQL_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "sql"))
_DSN_CACHE: dict[str, str] = {}


def _dsn(uri: str | None = None) -> str:
    return uri or config.SERVING_DB_URI


@contextlib.contextmanager
def pg_conn(uri: str | None = None, autocommit: bool = True):
    conn = psycopg2.connect(_dsn(uri))
    conn.autocommit = autocommit
    try:
        yield conn
    finally:
        conn.close()


def _jsonable(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return v


def rows_to_tuples(df, columns: Sequence[str]) -> list[tuple]:
    """Convert a Spark/pandas frame into psycopg2-ready tuples (no NaN, no Timestamp)."""
    out: list[tuple] = []
    for row in df.collect() if hasattr(df, "collect") else df:
        d = row.asDict() if hasattr(row, "asDict") else dict(row)
        out.append(tuple(_jsonable(d.get(c)) for c in columns))
    return out


# ----------------------------------------------------------------------- files
def read_sql_file(name: str) -> str:
    with open(os.path.join(SQL_DIR, name), encoding="utf-8") as fh:
        return fh.read()


def ensure_serving_schema(conn) -> None:
    """Apply the (idempotent) DDL for the serving tables.  Cheap, runs per job start."""
    ddl = read_sql_file("20_serving.sql")
    with conn.cursor() as cur:
        cur.execute(ddl)


# ------------------------------------------------------------------- postgres
def upsert_rows(
    table: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
    rows: Iterable[tuple],
    *,
    update_columns: Sequence[str] | None = None,
    dsn: str | None = None,
    fetch_alert_ids: bool = False,
) -> list[Any]:
    """`INSERT ... ON CONFLICT (key) DO UPDATE` — the OLTP twin of Iceberg MERGE."""
    rows = list(rows)
    if not rows:
        return []
    update_columns = list(update_columns or [c for c in columns if c not in key_columns])
    stmt = psql.SQL("""
        INSERT INTO {table} ({cols})
        VALUES %s
        ON CONFLICT ({keys}) DO UPDATE SET {sets}
        {ret}
    """).format(
        table=psql.Identifier(*table.split(".")),
        cols=psql.SQL(", ").join(map(psql.Identifier, columns)),
        keys=psql.SQL(", ").join(map(psql.Identifier, key_columns)),
        sets=psql.SQL(", ").join(
            psql.SQL("{} = EXCLUDED.{}").format(psql.Identifier(c), psql.Identifier(c))
            for c in update_columns
        ),
        ret=psql.SQL("RETURNING alert_id") if fetch_alert_ids else psql.SQL(""),
    )
    with pg_conn(dsn) as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, stmt.as_string(conn), rows, page_size=1000)
        return [r[0] for r in cur.fetchall()] if fetch_alert_ids else []


def insert_ignore(table: str, columns: Sequence[str], rows: list[tuple], dsn: str | None = None) -> int:
    if not rows:
        return 0
    stmt = psql.SQL("INSERT INTO {t} ({c}) VALUES %s ON CONFLICT DO NOTHING").format(
        t=psql.Identifier(*table.split(".")),
        c=psql.SQL(", ").join(map(psql.Identifier, columns)),
    )
    with pg_conn(dsn) as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, stmt.as_string(conn), rows, page_size=1000)
        return cur.rowcount


def read_table(query: str, params: tuple | None = None, dsn: str | None = None) -> list[dict]:
    with pg_conn(dsn) as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params or ())
        return [dict(_jsonable_row(r)) for r in cur.fetchall()]


def _jsonable_row(row: dict) -> dict:
    return {k: _jsonable(v) for k, v in row.items()}


# ---------------------------------------------------------------------- redis
_redis_client = None


def redis_client():
    """None means "Redis is optional and not reachable" — jobs keep running."""
    global _redis_client
    if os.environ.get("REDIS_DISABLED", "").lower() in {"1", "true"}:
        return None
    if _redis_client is not None:
        return _redis_client
    try:
        import redis

        _redis_client = redis.Redis.from_url(
            config.REDIS_URL, decode_responses=True, socket_connect_timeout=2,
            health_check_interval=30,
        )
        _redis_client.ping()
    except Exception as exc:  # pragma: no cover - depends on env
        print(f">>> Redis unavailable ({exc}); online feature store disabled", flush=True)
        _redis_client = None
    return _redis_client


def redis_enabled() -> bool:
    return redis_client() is not None


def write_features_to_redis(features: list[dict], prefix: str | None = None) -> int:
    """`feat:txn:<id>` (per-transaction feature vector) and `feat:card:<id>`
    (the newest rolling aggregates, what a scorer needs to re-evaluate a card)."""
    r = redis_client()
    if r is None or not features:
        return 0
    prefix = prefix or config.REDIS_FEATURE_PREFIX
    card_prefix = config.REDIS_CARD_PREFIX
    ttl = config.REDIS_TTL_SECONDS
    with r.pipeline(transaction=False) as pipe:
        for feat in features:
            txn_id = feat.get("transaction_id")
            if not txn_id:
                continue
            payload = json.dumps({k: _jsonable(v) for k, v in feat.items()}, separators=(",", ":"))
            pipe.set(f"{prefix}{txn_id}", payload, ex=ttl)
            card_id = feat.get("card_id")
            if card_id:
                pipe.set(f"{card_prefix}{card_id}", payload, ex=ttl)
        pipe.execute()
    return len(features)


def write_score_to_redis(scores: list[dict]) -> int:
    r = redis_client()
    if r is None or not scores:
        return 0
    with r.pipeline(transaction=False) as pipe:
        for s in scores:
            txn_id = s.get("transaction_id")
            if not txn_id:
                continue
            key = f"{config.REDIS_SCORE_PREFIX}{txn_id}"
            pipe.set(key, json.dumps({k: _jsonable(v) for k, v in s.items()}, separators=(",", ":")),
                     ex=config.REDIS_TTL_SECONDS)
            pipe.sadd("alerts:open", txn_id) if s.get("is_alert") else None
        pipe.execute()
    return len(scores)


def read_json_keys(keys: Sequence[str]) -> dict[str, dict]:
    r = redis_client()
    if r is None or not keys:
        return {}
    with r.pipeline(transaction=False) as pipe:
        for k in keys:
            pipe.get(k)
        values = pipe.execute()
    out: dict[str, dict] = {}
    for k, v in zip(keys, values, strict=True):
        if v:
            with contextlib.suppress(json.JSONDecodeError):
                out[k] = json.loads(v)
    return out


def delete_keys(pattern: str) -> int:
    r = redis_client()
    if r is None:
        return 0
    n = 0
    for key in r.scan_iter(match=pattern, count=500):
        r.delete(key)
        n += 1
    return n


# ----------------------------------------------------- feature/score payloads
FEATURE_COLUMNS_FOR_SERVING = (
    "transaction_id", "card_id", "customer_id", "merchant_id", "event_ts_ts", "amount",
    "txn_count_5min", "txn_count_1h", "amount_sum_5min", "amount_sum_1h", "amount_max_1h",
    "txn_count_24h", "amount_sum_24h", "distinct_merchants_1h", "online_txn_count_1h",
    "international_txn_count_1h", "hour_of_day", "channel", "country_mismatch",
)


def features_to_rows(df):
    """DataFrame -> list[dict] limited to what the online store needs (small payloads)."""
    cols = [c for c in FEATURE_COLUMNS_FOR_SERVING if c in df.columns]
    return [ {k: _jsonable(v) for k, v in row.asDict().items() if k in cols} for row in df.collect() ]


def write_feature_snapshot(df) -> int:
    """Persist the same vectors into Postgres so the API works even if Redis is cold."""
    cols = [c for c in FEATURE_COLUMNS_FOR_SERVING if c in df.columns]
    if not cols or "transaction_id" not in cols:
        return 0
    rows = rows_to_tuples(df.select(*cols), cols)
    update = [c for c in cols if c not in ("transaction_id",)]
    return len(upsert_rows("public.transactions_feature_store", cols, ["transaction_id"], rows,
                           update_columns=update))


def write_predictions(scores: list[dict], dsn: str | None = None) -> tuple[int, int]:
    """Persist `public.fraud_scores` (+ one row per alert in `public.fraud_alerts`)."""
    if not scores:
        return 0, 0
    score_cols = [
        "transaction_id", "card_id", "event_ts_ts", "amount", "model_version", "model_score",
        "rule_score", "final_score", "decision", "rule_hits", "feature_snapshot", "scored_at",
    ]
    rows = []
    for s in scores:
        snap = s.get("feature_snapshot")
        rows.append((
            s.get("transaction_id"), s.get("card_id"), _jsonable(s.get("event_ts_ts")),
            _jsonable(s.get("amount")), s.get("model_version"), _jsonable(s.get("model_score")),
            _jsonable(s.get("rule_score")), _jsonable(s.get("final_score")), s.get("decision"),
            list(s.get("rule_hits") or []),
            json.dumps(snap, separators=(",", ":")) if isinstance(snap, dict) else snap,
            _jsonable(s.get("scored_at")) or "now()",
        ))
    upsert_rows(
        "public.fraud_scores", score_cols, ["transaction_id"], rows,
        update_columns=[c for c in score_cols if c != "transaction_id"], dsn=dsn,
    )
    alert_cols = ["transaction_id", "card_id", "score", "decision", "reasons", "status"]
    alert_rows = [
        (s.get("transaction_id"), s.get("card_id"), _jsonable(s.get("final_score")), s.get("decision"),
         ", ".join(s.get("rule_hits") or []) or "model_score", "open")
        for s in scores if s.get("is_alert")
    ]
    insert_ignore("public.fraud_alerts", alert_cols, alert_rows, dsn=dsn)
    return len(rows), len(alert_rows)


def _as_array(values) -> list:
    if values is None:
        return []
    if isinstance(values, str):
        return [v for v in re.split(r"[,\]]", values.strip("[]")) if v]
    return list(values)


def dimension_source_uri(table: str) -> str:
    """JDBC URI of the *operational* copy of a dimension table (CDC's source)."""
    return config.pg_dsn(config.POSTGRES_DB) + f"?table=public.{table}"


def dimension_serving_columns(table: str) -> list[str]:
    """Columns the Postgres mirror of a lakehouse dimension may contain."""
    from . import dimensions

    return dimensions.iceberg_columns(table)


def read_dimensions(table: str) -> list[dict]:
    spec_columns = "*"
    return read_table(f"SELECT {spec_columns} FROM public.{table}")


def apply_dml_statements(statements: Sequence[str], dsn: str | None = None) -> int:
    with pg_conn(dsn) as conn, conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    return len(statements)


def top_cards_for_review(limit: int = 20, dsn: str | None = None) -> list[dict]:
    """Convenience query for the alert console / `make check`."""
    return read_table(
        """
        SELECT card_id,
               count(*)                AS hits,
               max(final_score)        AS max_score,
               max(event_ts_ts)        AS last_event
        FROM public.fraud_scores
        WHERE decision IN ('review', 'decline')
        GROUP BY card_id
        ORDER BY max_score DESC, hits DESC
        LIMIT %s
        """,
        (limit,),
        dsn=dsn,
    )
