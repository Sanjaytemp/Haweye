"""Deterministic fraud rules — the safety net under the ML model.

Real fraud stacks score with a model *and* with hard rules:
  * rules catch obvious, expensive patterns even before a model is trained;
  * rules are explainable ("declined: 7 transactions in 5 minutes");
  * the rule score is also a *feature* the model can lean on.

The SQL generator and the pure-python evaluator are two views of the SAME rule
list, and `tests/unit/test_rules.py` asserts they agree — so the rule you read
here is literally the rule the pipeline runs.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame


@dataclass(frozen=True)
class Rule:
    rule_id: str
    weight: float
    sql: str
    python: Callable[[Mapping[str, Any]], bool]
    why: str


def _num(features: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    val = features.get(key, default)
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _bool(features: Mapping[str, Any], key: str) -> bool:
    val = features.get(key)
    if isinstance(val, str):
        return val.strip().lower() in {"1", "true", "t", "yes"}
    return bool(val)


RULES: tuple[Rule, ...] = (
    Rule(
        rule_id="CARD_VELOCITY_5MIN",
        weight=0.30,
        sql="coalesce(txn_count_5min, 0) >= 5",
        python=lambda f: _num(f, "txn_count_5min") >= 5,
        why=">= 5 transactions on the card in the last 5 minutes",
    ),
    Rule(
        rule_id="AMOUNT_SPIKE_1H",
        weight=0.22,
        sql="coalesce(amount_sum_1h, 0) > 1500",
        python=lambda f: _num(f, "amount_sum_1h") > 1500,
        why="more than 1,500 spent on the card in the last hour",
    ),
    Rule(
        rule_id="EXTREME_TICKET",
        weight=0.18,
        sql="amount > 5000 OR (coalesce(amount_to_limit, 0) > 0.9)",
        python=lambda f: _num(f, "amount") > 5000 or _num(f, "amount_to_limit") > 0.9,
        why="single ticket above 5,000 or above 90% of the credit limit",
    ),
    Rule(
        rule_id="UNUSUAL_FOR_CARD",
        weight=0.16,
        sql="(coalesce(amount_zscore_24h, 0) > 4 AND amount > 200) "
            "OR (coalesce(amount_to_avg_1h, 0) > 8 AND amount > 150)",
        python=lambda f: (_num(f, "amount_zscore_24h") > 4 and _num(f, "amount") > 200)
        or (_num(f, "amount_to_avg_1h") > 8 and _num(f, "amount") > 150),
        why="amount wildly outside this card's normal pattern",
    ),
    Rule(
        rule_id="CNP_ABROAD",
        weight=0.20,
        sql="country_mismatch = TRUE AND (card_present IS NULL OR card_present = FALSE) "
            "AND coalesce(merchant_risk_score, 0) >= 0.6",
        python=lambda f: _bool(f, "country_mismatch") and not _bool(f, "card_present")
        and _num(f, "merchant_risk_score") >= 0.6,
        why="card-not-present abroad at a high-risk merchant",
    ),
    Rule(
        rule_id="NEW_CATEGORY_HIGH_AMOUNT",
        weight=0.14,
        sql="is_new_merchant_category = TRUE AND amount > 300",
        python=lambda f: _bool(f, "is_new_merchant_category") and _num(f, "amount") > 300,
        why="first-ever spend in this merchant category above 300",
    ),
    Rule(
        rule_id="CLOSED_MERCHANT",
        weight=0.45,
        sql="merchant_closed_hit = TRUE",
        python=lambda f: _bool(f, "merchant_closed_hit"),
        why="merchant is flagged closed/on-blocklist",
    ),
    Rule(
        rule_id="LIMIT_EXCEEDED",
        weight=0.25,
        sql="coalesce(ratio_to_hourly_limit, 0) > 1",
        python=lambda f: _num(f, "ratio_to_hourly_limit") > 1,
        why="amount exceeds the card's configured 1-hour limit",
    ),
)

RULE_IDS = [r.rule_id for r in RULES]
MAX_RULE_SCORE = sum(r.weight for r in RULES)


# ------------------------------------------------------------------ spark / sql
def rule_columns_sql() -> str:
    """SQL expressions: per-rule flags, `rule_score`, `rule_hits`."""
    parts = [f"({r.sql}) AS rule_{r.rule_id.lower()}" for r in RULES]
    terms = " + ".join(f"CASE WHEN {r.sql} THEN {r.weight} ELSE 0 END" for r in RULES)
    parts.append(f"({terms}) / {MAX_RULE_SCORE:.4f} AS rule_score")
    parts.append(
        "array_compact(array("
        + ", ".join(f"CASE WHEN {r.sql} THEN '{r.rule_id}' END" for r in RULES)
        + ")) AS rule_hits"
    )
    return ",\n  ".join(parts)


def apply_rules(df: DataFrame, view_name: str = "scored_rows") -> DataFrame:
    """Add `rule_<ID>`, `rule_score`, `rule_hits` columns to a feature DataFrame."""
    df.createOrReplaceTempView(view_name)
    sql = f"SELECT t.*, {rule_columns_sql()} FROM {view_name} t"
    return df.sparkSession.sql(sql)


def blend(model_score: float | None, rule_score: float, w_model: float = 0.75) -> float:
    """Final score = weighted blend of model + rules, clipped to [0, 1].

    `model_score is None` means "there is no model yet" (cold start, or the model
    failed to load) - and then the **rules carry the decision at full weight**.
    Diluting them by 0.25 instead would make a decline arithmetically impossible
    before the first training run, i.e. the control would not exist when you most
    need it.  The same rule is implemented in `blend_sql()` and in `api/serve.py`.
    """
    rule = max(0.0, min(1.0, float(rule_score)))
    if model_score is None:
        return rule
    return max(0.0, min(1.0, w_model * float(model_score) + (1 - w_model) * rule))


def blend_sql(model_col: str = "prediction", rule_col: str = "rule_score",
              w_model: float = 0.75) -> str:
    """`blend()` as Spark SQL, so the streaming job and this module cannot drift."""
    return (
        f"least(1.0, greatest(0.0, CASE WHEN {model_col} IS NULL THEN {rule_col} "
        f"ELSE {w_model:.2f} * {model_col} + {1 - w_model:.2f} * {rule_col} END))"
    )


def decide(score: float, threshold: float, alert_floor: float | None = None) -> dict[str, Any]:
    """Approve / review / decline + alert, shared by Spark job and REST API."""
    alert_floor = alert_floor if alert_floor is not None else max(threshold, 0.75)
    review_floor = threshold * 0.6
    if score >= alert_floor:
        decision = "decline"
    elif score >= threshold:
        decision = "review"
    elif score >= review_floor:
        decision = "monitor"
    else:
        decision = "approve"
    return {
        "decision": decision,
        "is_alert": decision in {"review", "decline"} or score >= alert_floor,
        "needs_case": decision in {"review", "decline"},
    }


# --------------------------------------------------------------- pure python
def evaluate_python(features: Mapping[str, Any]) -> dict[str, Any]:
    """Same rules as `rule_columns_sql`, evaluated on a dict (for tests + API)."""
    hits = [r.rule_id for r in RULES if r.python(features)]
    score = sum(r.weight for r in RULES if r.rule_id in hits) / MAX_RULE_SCORE
    return {"rule_hits": hits, "rule_score": round(score, 6),
            "flags": {f"rule_{r.rule_id.lower()}": r.python(features) for r in RULES}}
