"""Definitions for the *dimension* tables — the reference data that enrichment
and CDC both need.  One place defines the columns; Postgres DDL, the Iceberg
DDL, the generator and the CDC merger all read from here, so they cannot drift.
"""
from __future__ import annotations

MERCHANTS = {
    "name": "merchants",
    "pk": "merchant_id",
    "iceberg": "dim.merchants",
    "columns": [
        ("merchant_id", "text", True),
        ("merchant_name", "text", False),
        ("merchant_category", "text", False),
        ("merchant_country", "text", False),
        ("merchant_risk_score", "double", False),
        ("merchant_avg_ticket", "double", False),
        ("merchant_first_seen", "date", False),
        ("merchant_closed", "boolean", False),
        ("updated_at", "timestamp", False),
    ],
}

CARD_ACCOUNTS = {
    "name": "card_accounts",
    "pk": "card_id",
    "iceberg": "dim.card_accounts",
    "columns": [
        ("card_id", "text", True),
        ("customer_id", "text", False),
        ("issuer_country", "text", False),
        ("credit_limit", "double", False),
        ("txn_limit_1h", "double", False),
        ("travel_notice", "boolean", False),
        ("card_age_days", "int", False),
        ("customer_segment", "text", False),
        ("card_status", "text", False),
        ("updated_at", "timestamp", False),
    ],
}

TABLES = {t["name"]: t for t in (MERCHANTS, CARD_ACCOUNTS)}

#: business columns inside the Debezium `after`/`before` payload, per table
DIMENSION_BUSINESS_COLUMNS = {
    name: [c[0] for c in spec["columns"] if c[0] != "updated_at"] + ["updated_at"]
    for name, spec in TABLES.items()
}

SPARK_TYPES = {
    "text": "string",
    "double": "double",
    "int": "int",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "bigint": "bigint",
}


def postgres_ddl() -> str:
    """Source-of-truth DDL for the operational Postgres DB (dimension owners)."""
    nl = "\n  "
    parts = []
    for spec in TABLES.values():
        cols = ", ".join(
            f"{name} {ptype}" + (" PRIMARY KEY" if name == spec["pk"] else " NOT NULL")
            for name, ptype, _ in [(c[0], c[1], c[2]) for c in spec["columns"]]
        )
        parts.append(f"CREATE TABLE IF NOT EXISTS public.{spec['name']} ({nl}{cols}{nl});")
    return "\n\n".join(parts)


def iceberg_ddl(catalog: str) -> str:
    """Lakehouse tables: business columns + CDC bookkeeping columns."""
    parts = []
    for spec in TABLES.values():
        cols: list[str] = []
        for name, ptype, _ in spec["columns"]:
            spark_type = SPARK_TYPES[ptype]
            if name == "updated_at":
                cols.append("source_ts timestamp")
                cols.append("source_ts_ms bigint")
            else:
                cols.append(f"{name} {spark_type}")
        cols += [
            "op string",
            "source_db string",
            "source_table string",
            "ingest_ts timestamp",
        ]
        fqn = f"{catalog}.{spec['iceberg']}"
        nl = "\n  "
        body = ("," + nl).join(cols)
        parts.append(f"CREATE TABLE IF NOT EXISTS {fqn} ({nl}{body}{nl}) USING iceberg")
    return "\n\n".join(parts)


def iceberg_columns(table: str) -> list[str]:
    spec = TABLES[table]
    cols = [name for name, _, _ in spec["columns"] if name != "updated_at"]
    return cols + ["source_ts", "source_ts_ms", "op", "source_db", "source_table", "ingest_ts"]
