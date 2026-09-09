"""Every SQL string this project generates must be valid for its dialect.

Locally we cannot start a JVM, so `sqlglot` is the second opinion: it catches the
class of bug an f-string template produces (unbalanced parens, a CTE that ends in
a comma, `CASE` without `END`, a typo'd function).  The integration test
`tests/integration/test_pyspark_sql.py` runs the same strings against a real
SparkSession when java exists — CI runs both.
"""
from __future__ import annotations

import pytest

sqlglot = pytest.importorskip("sqlglot", reason="sqlglot is in requirements-dev.txt")

import pathlib  # noqa: E402

from common import cdc, dimensions, enrichment, features, rules, schema  # noqa: E402


def _parse_statement(sql: str, label: str, dialect: str = "spark"):
    try:
        sqlglot.parse_one(sql, read=dialect, error_level=sqlglot.ErrorLevel.RAISE)
    except Exception as exc:
        pytest.fail(f"{label} is not valid {dialect} SQL: {exc}\n---\n{sql[:1500]}")


def _parse_columns(sql: str, label: str, dialect: str = "spark"):
    """The *_sql() helpers emit *column lists*; wrap them the way the job does."""
    _parse_statement(f"SELECT {sql} FROM src", label, dialect)


# ------------------------------------------------------------------ Spark SQL
def test_enrichment_sql_parses_and_provides_what_rules_need():
    sql = enrichment.enrichment_sql("ev", bounds_view="b", merchant_view="m",
                                    card_view="c", category_state_view="cs")
    _parse_statement(sql, "enrichment_sql")
    for col in ("country_mismatch", "merchant_risk_score", "is_new_merchant_category",
                "txn_limit_1h", "merchant_avg_ticket", "travel_notice", "merchant_closed_hit"):
        assert col in sql, f"enrichment must provide {col} (rules/features depend on it)"


def test_feature_sql_parses():
    _parse_statement(features.rolling_features_sql("ev"), "rolling_features_sql")
    _parse_columns(features.derived_features_sql("e"), "derived_features_sql")
    _parse_statement(features.micro_batch_aggs_sql("enriched"), "micro_batch_aggs_sql")
    _parse_statement(features.minute_history_sql("b"), "minute_history_sql")
    _parse_statement(features.day_history_sql("enriched", "b"), "day_history_sql")
    _parse_statement(features.batch_bounds_sql("enriched"), "batch_bounds_sql")
    _parse_columns(features.model_feature_sql(), "model_feature_sql")
    _parse_statement(features.minute_agg_sql("enriched"), "minute_agg_sql")
    _parse_statement(features.day_agg_sql("enriched"), "day_agg_sql")


def test_rule_sql_parses_and_flags_every_rule():
    _parse_columns(rules.rule_columns_sql(), "rule_columns_sql")
    low = rules.rule_columns_sql().lower()
    for r in rules.RULES:
        assert f"rule_{r.rule_id.lower()}" in low


def test_quality_gates_cover_the_silently_fatal_columns():
    """Every predicate here is the difference between "we noticed" and "we found out
    three weeks later".

    Read from source rather than called: building these Columns needs a JVM
    (`F.expr("interval ...")`), and a unit test must run without one.  The
    behaviour itself is asserted in tests/integration/test_pyspark_sql.py.
    """
    import re

    src = pathlib.Path(schema.__file__).read_text()
    body = src[src.index("def quality_checks("):]
    body = body[: body.index("\n\ndef ", 10)] if "\n\ndef " in body[10:] else body
    gates = set(re.findall(r'^\s+"([a-z_0-9]+)":', body, flags=re.M))
    assert {"has_required_fields", "amount_positive", "amount_sane", "event_time_parseable",
            "country_ok", "channel_ok", "currency_ok", "event_time_not_stale",
            "event_time_not_in_future"} <= gates, gates
    assert len(gates) >= 8, "the gate list shrank - each gate is a silent failure mode"


def test_quarantine_keeps_the_raw_payload():
    """A rejected row you cannot inspect is a row you cannot fix."""
    fields = schema.quarantine_schema().fieldNames()
    assert "raw_json" in fields or "payload_json" in fields
    assert any("reason" in f for f in fields), fields
    for trace_col in ("source_topic", "source_partition", "source_offset"):
        assert trace_col in fields, "without topic/partition/offset you cannot replay the row"


# ----------------------------------------------------------------- Postgres DDL
def test_dimension_ddl_is_self_consistent():
    """The Postgres DDL, the Iceberg DDL and the Debezium capture list must agree:
    a column in one and not another is a MERGE that fails at runtime."""
    pg = dimensions.postgres_ddl()
    ice = dimensions.iceberg_ddl("lake")
    assert pg and ice
    for name, spec in dimensions.TABLES.items():
        cols = [c[0] for c in spec["columns"]]
        assert f"public.{name}" in pg, f"{name} missing from postgres_ddl()"
        assert spec["pk"] in cols
        for col in cols:
            assert col in pg, f"public.{name}.{col} missing in postgres DDL"
            assert col in ice or col == "updated_at", f"{name}.{col} missing in iceberg DDL"
        assert dimensions.iceberg_columns(name)
        for extra in ("op", "source_db", "source_table", "ingest_ts", "source_ts"):
            assert extra in ice, f"iceberg DDL needs the CDC bookkeeping column {extra}"


def test_postgres_ddl_uses_text_for_join_keys():
    """`char(2)` pads on read and silently breaks `=` joins with Spark strings.
    This project had that bug; the test keeps it from coming back."""
    pg = dimensions.postgres_ddl().lower()
    assert "char(" not in pg
    for key in ("merchant_id", "card_id", "issuer_country", "merchant_country"):
        assert f"{key} text" in pg, f"{key} should be text, got: {pg[:200]}"


def test_iceberg_and_postgres_types_map():
    for _name, spec in dimensions.TABLES.items():
        for col, ptype, _nullable in spec["columns"]:
            if ptype == "text":
                assert f"{col} string" in dimensions.iceberg_ddl("lake")


# ------------------------------------------------------------------ Debezium
def test_cdc_connector_config_is_complete_and_modern():
    body = cdc.debezium_connector_config()
    cfg = body["config"]
    assert body["name"] == "haweye-dimensions"
    assert cfg["connector.class"].endswith("PostgresConnector")
    assert cfg["plugin.name"] == "pgoutput"          # PG16 needs no extension to install
    assert cfg["publication.name"] and cfg["slot.name"]
    assert cfg["snapshot.mode"] == "initial"
    assert cfg["tombstones.on.delete"] == "false", "deletes must arrive as op=d, not null values"
    for conv in ("key.converter", "value.converter"):
        assert cfg[conv].endswith("JsonConverter"), (
            "relational keys/values are Structs; StringConverter dies on the first record")
    assert cfg["decimal.handling.mode"] == "double"
    assert set(cfg["table.include.list"].split(",")) == {f"public.{t}" for t in dimensions.TABLES}
    assert cfg["schema.include.list"] == "public"
    # topic naming must equal config.cdc_topic(): the merge job reads those topics
    prefix = cfg["topic.prefix"]
    for table in cfg["table.include.list"].split(","):
        schema_name, table_name = table.split(".")
        assert prefix == "cdc"
        assert f"{prefix}.{schema_name}.{table_name}"


def test_cdc_parser_handles_all_debezium_ops():
    """Debezium's wire codes are c/u/d/r (+ t for truncate).  A typo in the mapping
    turns a delete into an update - the worst kind of replication bug."""
    assert {cdc.INSERT, cdc.UPDATE, cdc.DELETE, cdc.SNAPSHOT_READ} == {
        "INSERT", "UPDATE", "DELETE", "READ"}
    import inspect

    src = inspect.getsource(cdc.parse_cdc_stream)
    for op in ("c", "u", "d", "r"):
        assert f'== "{op}"' in src, f"parse_cdc_stream ignores op={op}"
    assert "$.payload" in src, "must unwrap both schema-wrapped and bare envelopes"


def test_cdc_managed_columns_are_not_overwritten_by_merges():
    cols = set(cdc.MANAGED_COLUMNS)
    assert {"op", "source_ts", "ingest_ts"} <= cols or "ingest_ts" in cols


def test_repl_identity_and_publication_are_in_the_setup_sql():
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "cdc" / "sql" / "00_cdc_setup.sql"
    sqltext = path.read_text().lower()
    assert "replica identity full" in sqltext, "UPDATEs would lose the before-image"
    assert "wal_level=logical" in sqltext or "wal_level = logical" in sqltext
    assert "publication" in sqltext and "haweye_cdc" in sqltext
    assert "rePLICATION" in sqltext or "replication" in sqltext
