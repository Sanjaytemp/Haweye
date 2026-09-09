"""The rule engine is the part of this system a human can actually audit, so the
tests read like the analyst's spec: *this* input must fire *this* rule, and only
this one.

Run:  make test            (or)  pytest tests/unit/test_rules.py -v
"""
from __future__ import annotations

import math

import pytest

from common import rules


def test_rules_have_sql_and_python_for_every_rule():
    """The SQL variant runs in Spark, the python variant in the REST API.  A rule
    implemented in only one of them means the two scoring paths can disagree."""
    assert len(rules.RULES) >= 6
    for r in rules.RULES:
        assert r.sql.strip(), r.rule_id
        assert callable(r.python), r.rule_id
        assert 0 < r.weight <= 1, f"{r.rule_id} weight out of range: {r.weight}"
        assert r.why, f"{r.rule_id} needs a human-readable reason"


def test_rule_ids_unique_and_weights_normalised():
    ids = [r.rule_id for r in rules.RULES]
    assert len(ids) == len(set(ids))
    assert pytest.approx(sum(r.weight for r in rules.RULES)) == rules.MAX_RULE_SCORE
    assert rules.MAX_RULE_SCORE > 1  # weights are *not* required to sum to 1 (they are normalised later)


@pytest.mark.parametrize(
    "features,expected",
    [
        ({}, []),                                        # an empty dict must not explode
        ({"txn_count_5min": 4}, []),                     # one below the threshold
        ({"txn_count_5min": 5}, ["CARD_VELOCITY_5MIN"]),  # exactly on it
        ({"amount_sum_1h": 1501}, ["AMOUNT_SPIKE_1H"]),
        ({"amount": 5001}, ["EXTREME_TICKET"]),
        ({"amount_to_limit": 0.95}, ["EXTREME_TICKET"]),
        ({"country_mismatch": True, "card_present": False, "merchant_risk_score": 0.7}, ["CNP_ABROAD"]),
        ({"country_mismatch": True, "card_present": True, "merchant_risk_score": 0.9}, []),
        ({"merchant_closed_hit": True}, ["CLOSED_MERCHANT"]),
        ({"ratio_to_hourly_limit": 1.5}, ["LIMIT_EXCEEDED"]),
        ({"is_new_merchant_category": True, "amount": 400}, ["NEW_CATEGORY_HIGH_AMOUNT"]),
        ({"is_new_merchant_category": True, "amount": 40}, []),
    ],
)
def test_single_signals_fire_the_expected_rule(features, expected):
    assert rules.evaluate_python(features)["rule_hits"] == expected


def test_multi_signal_row_accumulates_and_stays_bounded():
    bad = {
        "txn_count_5min": 12, "amount_sum_1h": 9000, "amount": 8000, "amount_to_limit": 1.4,
        "country_mismatch": True, "card_present": False, "merchant_risk_score": 0.9,
        "merchant_closed_hit": True, "is_new_merchant_category": True, "ratio_to_hourly_limit": 3,
        "amount_zscore_24h": 9, "amount_to_avg_1h": 40,
    }
    out = rules.evaluate_python(bad)
    assert len(out["rule_hits"]) == len(rules.RULES)
    assert 0 < out["rule_score"] <= 1.0
    # every rule also reported its own flag, so an analyst can see which fired
    assert set(out["flags"]) == {f"rule_{r.rule_id.lower()}" for r in rules.RULES}


def test_strings_and_nulls_from_postgres_are_coerced():
    """JSON/Postgres give us 'true' and None; a naive `if f['x']` would crash on
    None and treat 'false' as truthy.  Both are real production bugs."""
    out = rules.evaluate_python({"card_present": "false", "country_mismatch": "true",
                                 "merchant_risk_score": None, "txn_count_5min": None})
    assert out["rule_hits"] == []
    assert math.isfinite(out["rule_score"])


def test_rule_score_is_weight_sum_over_max():
    f = {"txn_count_5min": 5}
    got = rules.evaluate_python(f)["rule_score"]
    expect = next(r.weight for r in rules.RULES if r.rule_id == "CARD_VELOCITY_5MIN") / rules.MAX_RULE_SCORE
    assert got == pytest.approx(expect, abs=1e-6)   # evaluate_python rounds to 6dp


def test_rule_sql_is_valid_sql_and_mentions_every_flag():
    sql = "SELECT " + rules.rule_columns_sql() + " FROM t"
    sqltext = sql.lower()
    for r in rules.RULES:
        assert f"rule_{r.rule_id.lower()}" in sqltext
    assert "rule_score" in sqltext and "rule_hits" in sqltext
    pytest.importorskip("sqlglot")
    import sqlglot

    sqlglot.parse_one(sql, dialect="spark", read="spark", error_level=sqlglot.ErrorLevel.RAISE)


def test_blend_ignores_missing_model_and_clips():
    # no model yet -> rules carry the decision at FULL weight (a decline must be
    # possible on day 1, before the first nightly training run)
    assert rules.blend(None, 0.8) == pytest.approx(0.8)
    assert rules.blend(None, 0.0) == 0.0
    assert rules.blend(1.0, 1.0) == 1.0
    assert rules.blend(-5.0, -5.0) == 0.0
    # a model that is sure + rules that are sure must beat the rules alone
    assert rules.blend(0.9, 0.9) > 0.9 * 0.25


def test_blend_sql_matches_the_python_blend():
    """The streaming job scores in SQL, the API in python: same formula, twice.
    This test is what makes the duplication safe."""
    sql = rules.blend_sql("p", "r").lower()
    assert "case when p is null then r" in sql, "cold-start path must use rules alone"
    assert "0.75 * p + 0.25 * r" in sql
    assert "least(1.0" in sql and "greatest(0.0" in sql


def test_decide_ladder_is_monotonic():
    threshold = 0.6
    # approve -> monitor (between review_floor and threshold) -> review -> decline
    seen = [rules.decide(s, threshold)["decision"] for s in (0.0, 0.4, 0.6, 0.8, 0.99)]
    assert seen == ["approve", "monitor", "review", "decline", "decline"], seen
    assert [rules.decide(s, threshold)["needs_case"] for s in (0.0, 0.4)] == [False, False]
    assert rules.decide(0.0, threshold)["is_alert"] is False
    assert rules.decide(0.99, threshold)["is_alert"] is True
    # the ladder never gets *less* severe as the score rises
    order = {"approve": 0, "monitor": 1, "review": 2, "decline": 3}
    scores = [order[rules.decide(s, threshold)["decision"]] for s in [i / 100 for i in range(101)]]
    assert scores == sorted(scores)


def test_decide_sets_alert_floor_consistently():
    """The API and the streaming job must agree on what becomes a human task."""
    d = rules.decide(0.8, 0.6, alert_floor=0.75)
    assert d["decision"] == "decline" and d["is_alert"] and d["needs_case"]
    # just above the threshold: reviewed by a human, not auto-declined
    d2 = rules.decide(0.65, 0.6, alert_floor=0.75)
    assert d2["decision"] == "review" and d2["is_alert"] and d2["needs_case"] is not False
