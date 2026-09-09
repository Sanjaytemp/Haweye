"""Feature definitions are the highest-risk code in this repo: a wrong window is
not an error, it is a *model that quietly learns the wrong thing*.

Each test states the semantics in plain language and checks the generator code
against a dumb reference implementation.  `tests/integration/test_pyspark_sql.py`
runs the same SQL for real on a JVM; CI does both.
"""
from __future__ import annotations

import pathlib

import pandas as pd
import pytest

from common import features


def _events():
    """One card: 10:00 (10), 10:03 (20), 10:06 (400)."""
    return pd.DataFrame({
        "card_id": ["C1", "C1", "C1"],
        "event_ts_ts": pd.to_datetime([
            "2024-06-01T10:00:00", "2024-06-01T10:03:00", "2024-06-01T10:06:00"]),
        "amount": [10.0, 20.0, 400.0],
        "merchant_id": ["M1", "M2", "M1"],
    })


def test_pandas_oracle_never_looks_at_the_future():
    df = features.rolling_features_pandas(_events())
    # 10:06 - 5min = 10:01, so the 10:03 event *is* inside; the 10:00 one is not.
    assert df.loc[2, "txn_count_5min"] == 2
    assert df.loc[2, "amount_sum_5min"] == pytest.approx(420.0)
    assert df.loc[2, "txn_count_1h"] == 3
    assert df.loc[2, "amount_sum_1h"] == pytest.approx(430.0)
    assert df.loc[2, "amount_max_1h"] == pytest.approx(400.0)
    # the *first* row can only see itself: this is the entire point of an as-of join
    assert df.loc[0, "txn_count_1h"] == 1
    assert df.loc[0, "amount_sum_1h"] == pytest.approx(10.0)


def test_windows_are_per_card():
    ev = pd.concat([_events(), _events().assign(card_id="C2", amount=[1000.0, 2000.0, 3000.0])],
                   ignore_index=True)
    df = features.rolling_features_pandas(ev)
    assert df.loc[0, "amount_sum_1h"] == pytest.approx(10.0)
    assert df.loc[3, "amount_sum_1h"] == pytest.approx(1000.0)


def test_filtered_windows_use_the_model_names():
    """Regression guard: `online_1h`/`intl_1h` are rendered as `online_*` /
    `international_*` so the rolling SQL, the minute-history pivot, the feature
    table schema and NUMERIC_FEATURES cannot drift apart.  (They did, once.)"""
    names = features.all_rolling_feature_names()
    assert "online_txn_count_1h" in names
    assert "international_txn_count_1h" in names
    assert "txn_count_online_1h" not in names
    sql = features.micro_batch_aggs_sql("ev")
    assert "AS online_txn_count_1h" in sql and "AS international_txn_count_1h" in sql


def test_model_numeric_features_exist_in_the_feature_table():
    """If a model input is missing from the table, `ensure_feature_columns` fills
    0 and training 'succeeds' on a dead feature.  Never let that be silent."""
    table = features.FEATURES_TABLE_SCHEMA.fieldNames()
    missing = set(features.NUMERIC_FEATURES) - set(table)
    assert not missing, f"model inputs absent from the feature table: {missing}"


def test_model_feature_order_is_stable_and_unique():
    model = features.model_feature_names()
    assert model == list(dict.fromkeys(model)), "duplicate names would double-count"
    assert model == list(features.NUMERIC_FEATURES) + [f"{c}_idx" for c in features.CATEGORICAL_FEATURES]
    # the `_idx` columns are created by the StringIndexer inside the pipeline, so
    # their *source* columns must exist in the table instead
    for c in features.CATEGORICAL_FEATURES:
        assert c in features.FEATURES_TABLE_SCHEMA.fieldNames()


def test_derived_sql_guards_every_division():
    sql = features.derived_features_sql("t")
    lines = sql.splitlines()
    bare = [ln for i, ln in enumerate(lines)
            if "/" in ln and "CASE" not in ln.upper() and "CASE" not in lines[i - 1].upper()]
    assert not bare, f"division without a zero guard: {bare}"
    assert sql.upper().count("CASE WHEN") >= sql.count("/") - 1


def test_rolling_sql_orders_by_event_time_in_seconds():
    sql = features.rolling_features_sql("ev").lower()
    assert "partition by e.card_id" in sql
    assert "unix_timestamp(event_ts_ts)" in sql, "RANGE frames need a numeric ordering"
    for _suffix, secs, _f in features.ROLLING_WINDOWS:
        assert f"range between {secs} preceding and current row" in sql
    assert "over (partition by e.card_id order by e.__ts" in sql


def test_day_window_is_served_from_history_not_a_scan():
    """24h per-row windows are the classic streaming-OOM; the design keeps them in
    an aggregate table (`raw.card_minute_agg` / `card_day_agg`)."""
    micro = features.micro_batch_aggs_sql("ev")
    assert "86400" not in micro, "the micro-batch SQL must not compute 24h windows"
    # the aggregate tables themselves are written by the job, not by the SQL helper
    job_src = (pathlib.Path(__file__).resolve().parents[2] / "jobs" / "feature_store.py").read_text()
    assert "features.DAY_TABLES" in job_src, "the job must write through the registry, not literals"
    assert "MERGE INTO" in job_src, "history tables must be upserted (idempotent reruns)"
    assert set(features.DAY_TABLES) == {"minute", "day"}
    assert features.DAY_TABLES["minute"].startswith("raw.")


def test_feature_table_schema_matches_builder():
    schema = features.features_table_schema()
    assert [f.name for f in schema.fields] == features.FEATURES_TABLE_SCHEMA.fieldNames()
    assert "feature_json" not in schema.fieldNames()   # JSON is the *API* projection, see io.py
    for key in ("transaction_id", "card_id", "event_ts_ts"):
        assert key in schema.fieldNames()


def test_enrichment_output_names_cover_what_rules_and_models_need():
    from common import enrichment

    cols = set(enrichment.enriched_columns())
    need = {"country_mismatch", "merchant_risk_score", "is_new_merchant_category",
            "merchant_closed_hit", "travel_notice", "ratio_to_hourly_limit",
            "ratio_to_merchant_avg"}
    assert need <= cols, need - cols


def test_ensure_feature_columns_null_handling_is_documented_behaviour():
    """NULL -> 0 is a *choice*; it must be visible in the SQL the model is built from."""
    sql = features.model_feature_sql()
    assert sql.count("coalesce") >= len(features.NUMERIC_FEATURES)
    assert "AS amount" in sql
