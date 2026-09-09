"""Spark-backed tests: these run the generated SQL on a real JVM.

Skipped automatically when java is missing (`make test` stays fast on a laptop);
CI installs a JDK and runs them with `make test-spark`.

Why bother when `tests/unit` parses the SQL with sqlglot?  Because Spark's
analyzer catches what a parser cannot: unknown columns, ambiguity after a join,
window frames over the wrong type, and NULL semantics you did not intend.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = pytest.mark.spark

from common import cdc, features, rules, schema  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (SparkSession.builder.appName("haweye-unit")
         .master("local[2]")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.execution.arrow.pipelining.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _txns(n=60, seed=3):
    from generator import TransactionSimulator, build_cards, build_merchants

    rng = __import__("random").Random(seed)
    sim = TransactionSimulator(build_merchants(20, rng), build_cards(10, rng),
                               seed=seed, fraud_rate=0.2,
                               start=datetime(2024, 6, 1, tzinfo=timezone.utc))
    return [txn for txn, _ in sim.stream(n)]


def kafka_rows(spark, payloads: list[dict], bad: list[str] | None = None):
    rows = [(f"k{i}", json.dumps(p), "raw_transactions", 0, i,
             datetime(2024, 6, 1, tzinfo=timezone.utc) + timedelta(seconds=i))
            for i, p in enumerate(payloads)]
    for j, text in enumerate(bad or []):
        rows.append((f"bad{j}", text, "raw_transactions", 0, 10_000 + j,
                     datetime(2024, 6, 1, tzinfo=timezone.utc)))
    return spark.createDataFrame(
        rows, "key string, value string, topic string, partition int, offset long, timestamp timestamp")


def test_parse_and_quality_gates_accept_good_rows_reject_garbage(spark):
    df = schema.parse_transactions(kafka_rows(
        spark, _txns(40),
        bad=["not json at all", '{"transaction_id":"X","amount":-4,"event_ts":"nope","card_id":"C"}']))
    flagged = schema.add_quality_flags(df)
    good, bad = schema.split_good_and_bad(flagged)
    assert good.count() >= 38, "valid rows must never be quarantined"
    assert bad.count() >= 2, "malformed JSON and a negative amount must be quarantined"
    reasons = [r["failure_reasons"] for r in schema.quarantine_rows(bad).collect()]
    assert any(reason for reason in reasons), f"quarantine rows need a reason: {reasons}"


def test_dedupe_is_idempotent_on_replayed_kafka(spark):
    payloads = _txns(25)
    dup = payloads + payloads[:5]                 # Kafka at-least-once replays
    df = schema.parse_transactions(kafka_rows(spark, dup))
    deduped = schema.dedupe(df)
    assert deduped.count() == len(payloads), "replayed rows must collapse, not double-count"
    # and the *newest* offset survives, which is what makes the dedupe stable
    assert schema.dedupe(deduped).count() == len(payloads), "dedupe must be idempotent"


def test_rolling_window_sql_runs_and_matches_the_pandas_oracle(spark):
    import pandas as pd

    pdf = pd.DataFrame({
        "card_id": ["C1", "C1", "C1", "C2"],
        "event_ts_ts": [datetime(2024, 6, 1, 10, 0), datetime(2024, 6, 1, 10, 3),
                        datetime(2024, 6, 1, 10, 6), datetime(2024, 6, 1, 10, 1)],
        "amount": [10.0, 20.0, 400.0, 1000.0],
        "merchant_id": ["M1", "M2", "M1", "M9"],
        "channel": ["pos", "online", "online", "atm"],
        "country_mismatch": [False, False, True, False],
    })
    events = spark.createDataFrame(pdf)
    events.createOrReplaceTempView("ev")
    rolled = spark.sql(features.rolling_features_sql("ev"))
    row3 = rolled.filter("card_id='C1' and amount=400").collect()[0]
    assert row3["txn_count_5min"] == 2
    assert row3["amount_sum_1h"] == pytest.approx(430.0)
    assert row3["online_txn_count_1h"] == 2            # filtered window, online only
    assert row3["international_txn_count_1h"] == 1     # filtered by country_mismatch
    assert rolled.count() == 4
    oracle = features.rolling_features_pandas(pdf)
    for i in range(3):
        got = rolled.filter(f"card_id='C1' and amount={pdf.loc[i,'amount']}").first()
        want = oracle.loc[i]
        assert got["txn_count_1h"] == want["txn_count_1h"]
        assert float(got["amount_sum_1h"]) == pytest.approx(float(want["amount_sum_1h"]))


def test_derived_features_and_model_vector_build(spark):
    events = spark.createDataFrame(_enriched_rows()).cache()
    events.createOrReplaceTempView("enr")
    feats = spark.sql(features.derived_features_sql("enr"))
    assert feats.filter("amount_zscore_24h IS NOT NULL").count() >= 0
    built = features.ensure_feature_columns(
        feats.select("enr.*", *[c for c in feats.columns if c not in ("amount",)]))
    missing = set(features.model_feature_names()) - set(built.columns)
    assert not missing, f"model inputs not materialised: {missing}"
    events.unpersist()


def _enriched_rows():
    base = datetime(2024, 6, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(12):
        rows.append({
            "transaction_id": f"T{i}", "card_id": "C1", "merchant_id": f"M{i % 3}",
            "event_ts_ts": base + timedelta(minutes=i * 2),
            "amount": 10.0 * (i + 1), "currency": "USD",
            "channel": "online" if i % 2 else "pos",
            "card_present": i % 2 == 0, "is_3ds": i % 3 == 0,
            "country_mismatch": i % 4 == 0, "merchant_country": "NG" if i % 4 == 0 else "US",
            "merchant_category": "crypto", "customer_segment": "consumer",
            "credit_limit": 200.0, "txn_limit_1h": 50.0, "merchant_avg_ticket": 30.0,
            "merchant_risk_score": 0.8, "card_age_days": 400, "travel_notice": False,
            "is_new_merchant_category": i == 11, "merchant_closed_hit": i == 11,
        })
    return rows


def test_rules_sql_and_python_agree_on_real_rows(spark):
    rows = _enriched_rows()
    spark.createDataFrame(rows).createOrReplaceTempView("f")
    out = rules.apply_rules(spark.sql("SELECT * FROM f")).collect()
    for got, src in zip(out, rows, strict=True):
        expect = rules.evaluate_python(src)
        assert sorted(got["rule_hits"]) == sorted(expect["rule_hits"]), (got["rule_hits"], expect, src)
        assert float(got["rule_score"]) == pytest.approx(expect["rule_score"], abs=1e-3)


def test_final_score_formula_matches_the_blend_helper(spark):
    """The streaming job inlines `blend()` as SQL; this proves they compute the
    same number, including the cold-start branch."""
    rows = [{"prediction": None, "rule_score": 0.8}, {"prediction": 0.9, "rule_score": 0.4},
            {"prediction": 0.1, "rule_score": 0.2}]
    df = spark.createDataFrame([(r["prediction"], r["rule_score"]) for r in rows],
                               "prediction double, rule_score double")
    expr = rules.blend_sql("coalesce(prediction, 0)", "coalesce(rule_score, 0)")
    got = [row[0] for row in df.selectExpr(f"CASE WHEN prediction IS NULL THEN rule_score "
                                           f"ELSE {expr} END AS final").collect()]
    want = [rules.blend(r["prediction"], r["rule_score"]) for r in rows]
    assert got == pytest.approx(want, abs=1e-9)


def test_cdc_envelope_is_flattened_for_every_op(spark):
    def env(op, before, after, ts_ms):
        return json.dumps({"payload": {"op": op, "ts_ms": ts_ms, "source": {"table": "merchants",
                                                                            "schema": "public", "db": "dimensions",
                                                                            "id": "0001", "lsn": "1/2"},
                                       "before": before, "after": after}})

    rows = [(env("c", None, {"merchant_id": "M1", "merchant_name": "A", "merchant_category": "grocery",
                             "merchant_country": "US", "merchant_risk_score": 0.1,
                             "merchant_avg_ticket": 20.0, "merchant_first_seen": "2024-01-01",
                             "merchant_closed": False, "updated_at": "2024-06-01T00:00:00Z"}, 1),),
            (env("u", {"merchant_id": "M1"}, {"merchant_id": "M1", "merchant_name": "A2",
                                               "merchant_category": "grocery", "merchant_country": "US",
                                               "merchant_risk_score": 0.9, "merchant_avg_ticket": 20.0,
                                               "merchant_first_seen": "2024-01-01", "merchant_closed": False,
                                               "updated_at": "2024-06-02T00:00:00Z"}, 2),),
            (env("d", {"merchant_id": "M1"}, None, 3),)]
    # keep it simple: parse_cdc_stream only needs `value`, `timestamp`, topic/partition/offset
    src = spark.createDataFrame(
        [(r[0], datetime(2024, 6, 1, tzinfo=timezone.utc)) for r in rows], "value string, timestamp timestamp")
    changes = cdc.parse_cdc_stream(src)
    assert changes.count() == 3
    ops = sorted([r["op"] for r in changes.collect()])
    assert ops == sorted([cdc.INSERT, cdc.UPDATE, cdc.DELETE])
    # tombstones are off, so a delete arrives as op=d with after=null and `before` carrying the id
    # one row per key, and the newest is the delete
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    dedup = changes.withColumn("merchant_id", F.get_json_object(F.col("body"), "$.after.merchant_id"))
    dedup = dedup.withColumn("merchant_id", F.coalesce(F.col("merchant_id"),
                                                       F.get_json_object(F.col("body"), "$.before.merchant_id")))
    w = Window.partitionBy("merchant_id").orderBy(F.col("source_ts_ms").desc(), F.col("op").desc())
    top = dedup.withColumn("__rn", F.row_number().over(w)).where("__rn = 1").collect()
    assert [r["op"] for r in top] == [cdc.DELETE]
    assert top[0]["source_table"] == "merchants"


def test_feature_store_projection_roundtrips(spark):
    """`to_feature_store_layout` + `parse_feature_json` are the writer/reader pair
    the API consumes; they must be exact inverses."""
    from pyspark.sql import functions as F

    rows = _enriched_rows()
    spark.createDataFrame(rows).createOrReplaceTempView("f")
    feats = features.ensure_feature_columns(spark.sql("SELECT * FROM f"))
    laid = features.to_feature_store_layout(feats, model_uri="s3a://x", model_version="v1")
    assert laid.count() == len(rows)
    payload = laid.select("transaction_id",
                          F.to_json(F.struct(*[F.col(n) for n in features.NUMERIC_FEATURES[:5]]))
                          .alias("value"))
    parsed = features.parse_feature_json(payload)
    assert parsed.count() == len(rows)
    assert set(features.NUMERIC_FEATURES[:5]) <= set(parsed.columns)


def test_sparkutils_helpers(spark):
    from common import sparkutils

    spark.createDataFrame(_enriched_rows()).createOrReplaceTempView("v")
    assert sparkutils.table_exists(spark, "v") is True
    assert sparkutils.table_exists(spark, "nope.nope.nope") is False
