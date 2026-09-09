"""The REST API must be testable with *nothing running*: no Redis, no Postgres,
no model artifact.  That is not laziness - it is the property that makes this
service deployable (and it is exactly how CI can test it without docker-in-docker).

Run:  make test   (or)  pytest tests/unit/test_api_contract.py -v
"""
from __future__ import annotations

import json
import random

import pytest

serve = pytest.importorskip("serve", reason="pip install fastapi uvicorn redis psycopg2-binary")


class FakeRedis:
    """Only `get`/`ping` are real; anything else returns None so we discover
    endpoints that assume a Redis reply is always present."""

    def __init__(self, data: dict[str, str] | None = None) -> None:
        self.data = data or {}

    def ping(self) -> bool:
        return True

    def get(self, key: str):
        return self.data.get(key)

    def __getattr__(self, name):          # keys/hgetall/zrevrange/...
        def _empty(*args, **kwargs):
            return None
        return _empty


@pytest.fixture
def api(monkeypatch):
    calls: list[tuple[str, tuple]] = []

    def fake_query(sqltext, params=()):
        calls.append((sqltext, tuple(params)))
        if "FROM public.fraud_alerts" in sqltext:
            return [{"alert_id": 1, "status": "open", "card_id": "CARD-1", "final_score": 0.9}]
        return []

    monkeypatch.setattr(serve, "_redis", FakeRedis())
    monkeypatch.setattr(serve, "query", fake_query)
    monkeypatch.setattr(serve, "MODEL", serve.ModelHolder())   # nothing on disk -> rules only
    monkeypatch.setattr(serve, "API_TOKEN", "")
    from fastapi.testclient import TestClient

    client = TestClient(serve.app)
    return client, calls


def test_healthz_reports_degraded_dependencies_without_crashing(api):
    client, _ = api
    r = client.get("/healthz")
    assert r.status_code in (200, 503), r.text        # degraded is still an answer
    body = r.json()
    assert "redis" in body and "postgres" in body and "model" in body


def test_scoring_a_benign_transaction_approves(api):
    client, _ = api
    r = client.post("/v1/score", json={"card_id": "CARD-00001", "merchant_id": "MER-0001",
                                       "amount": 12.5, "channel": "pos",
                                       "merchant_country": "US", "card_present": True})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["decision"] == "approve", body
    assert body["rule_hits"] == []
    assert body["model_score"] is None, "no artifact on disk -> model must be skipped, not faked"
    assert body["latency_ms"] >= 0


def test_scoring_an_obvious_attack_is_not_approved(api, monkeypatch):
    client, _ = api
    # Redis holds this card's rolling state: 6 transactions in 5 min, a fat hour
    online = {"txn_count_5min": 5, "txn_count_1h": 9, "amount_sum_1h": 4000.0,
              "amount_std_24h": 40.0, "amount_avg_24h": 60.0, "credit_limit": 5000.0,
              "txn_limit_1h": 600.0, "issuer_country": "US", "merchant_avg_ticket": 40.0,
              "is_new_merchant_category": True, "merchant_risk_score": 0.9,
              "merchant_closed_hit": False}
    monkeypatch.setattr(serve, "_redis", FakeRedis({"feat:card:CARD-00042": json.dumps(online)}))
    r = client.post("/v1/score", json={"card_id": "CARD-00042", "merchant_id": "MER-0007",
                                       "amount": 4900, "channel": "online",
                                       "merchant_country": "NG", "card_present": False,
                                       "is_3ds": False})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["decision"] in {"review", "decline"}, body
    assert "CARD_VELOCITY_5MIN" in body["rule_hits"]
    assert body["rule_score"] > 0.3
    assert body["features_used"]["__source"].startswith("redis:")


def test_validation_rejects_impossible_input(api):
    client, _ = api
    assert client.post("/v1/score", json={"card_id": "C", "amount": -1}).status_code == 422
    assert client.post("/v1/score", json={"amount": 5}).status_code == 422   # card_id required


def test_unknown_transaction_is_a_404_not_a_500(api):
    client, _ = api
    r = client.get("/v1/score/TXN-DOES-NOT-EXIST")
    assert r.status_code == 404
    assert "pipeline" in r.json()["detail"].lower() or "score" in r.json()["detail"].lower()


def test_redis_outage_degrades_to_postgres_then_empty(api, monkeypatch):
    class Dead:
        def get(self, key):
            raise ConnectionError("connection refused")

        def ping(self):
            raise ConnectionError("connection refused")

    monkeypatch.setattr(serve, "_redis", Dead())
    client, _ = api
    r = client.post("/v1/score", json={"card_id": "CARD-1", "amount": 20.0})
    assert r.status_code == 200
    assert r.json()["features_used"]["__source"] == "none", "must say *why* features are empty"
    health = client.get("/healthz").json()
    assert str(health["redis"]).startswith(("unavailable", "ok"))


def test_api_token_is_enforced_when_configured(api, monkeypatch):
    monkeypatch.setattr(serve, "API_TOKEN", "s3cret")
    from fastapi.testclient import TestClient

    client = TestClient(serve.app)
    assert client.post("/v1/score", json={"card_id": "C", "amount": 1}).status_code == 401
    ok = client.post("/v1/score", json={"card_id": "C", "amount": 1},
                     headers={"X-API-Token": "s3cret"})
    assert ok.status_code == 200, ok.text


def test_scores_are_persisted_with_the_feature_snapshot(api):
    """Without the snapshot you cannot explain a decline to a customer 3 days later."""
    client, calls = api
    client.post("/v1/score", json={"card_id": "CARD-9", "amount": 4200, "channel": "online"})
    inserts = [c for c in calls if "INSERT INTO public.fraud_scores" in c[0]]
    assert inserts, "the hypothetical score must still land in Postgres"
    params = inserts[0][1]
    snapshot = json.loads(params[10])
    assert snapshot["amount"] == 4200.0
    assert "__source" not in snapshot, "internal provenance keys must not be persisted"


def test_model_endpoints_answer_without_a_model(api):
    client, _ = api
    body = client.get("/v1/model").json()
    assert body["status"]["loaded"] is False
    assert body["status"]["model_dir"].endswith("models")
    assert body["status"]["pointer_version"] is None
    r = client.post("/v1/model/refresh")
    assert r.status_code == 503, "an API that pretends to have a model is worse than 503"


def test_alerts_are_listable_and_actionable(api):
    """Case management is the human half of a fraud system; the API is its UI."""
    client, calls = api
    body = client.get("/v1/alerts").json()
    assert body["count"] == 1 and body["alerts"][0]["status"] == "open"
    ok = client.patch("/v1/alerts/1?status=confirmed&assignee=ana")
    assert ok.status_code == 200, ok.text
    assert ok.json() == {"alert_id": 1, "status": "confirmed"}
    update = [c for c in calls if "UPDATE public.fraud_alerts" in c[0]]
    assert update and "closed_at" in update[0][0], "closing must stamp the close time"
    # an unknown status is refused before it reaches SQL
    assert client.patch("/v1/alerts/1?status=whatever").status_code == 422


# ------------------------------------------------------------- rule parity
def test_api_rules_and_spark_rules_agree_on_random_rows():
    """Two implementations of the same policy MUST agree; otherwise the streaming
    job declines what the API approves.  Randomised because hand-picked cases
    hide exactly the boundary conditions that matter."""
    from common import rules as spark_rules

    rng = random.Random(1234)
    keys = ["txn_count_5min", "amount_sum_1h", "amount", "amount_to_limit", "amount_zscore_24h",
            "amount_to_avg_1h", "country_mismatch", "card_present", "merchant_risk_score",
            "is_new_merchant_category", "merchant_closed_hit", "ratio_to_hourly_limit"]
    mismatch = 0
    for _ in range(3000):
        f = {}
        for k in keys:
            if k in {"country_mismatch", "card_present", "is_new_merchant_category", "merchant_closed_hit"}:
                f[k] = rng.random() < 0.5
            else:
                f[k] = round(rng.choice([0, rng.random() * 6000, rng.uniform(0, 2)]), 3)
        a_hits, a_score = serve.evaluate_rules(f)
        b = spark_rules.evaluate_python(f)
        if sorted(a_hits) != sorted(b["rule_hits"]) or abs(a_score - b["rule_score"]) > 1e-3:
            mismatch += 1
            if mismatch == 1:
                pytest.fail(f"rule engines disagree on {f}\napi={a_hits},{a_score} spark={b}")
    assert mismatch == 0


def test_api_rule_table_matches_spark_weights():
    from common import rules as spark_rules

    api_weights = {r["rule_id"]: r["weight"] for r in serve.RULES}
    spark_weights = {r.rule_id: r.weight for r in spark_rules.RULES}
    assert api_weights == spark_weights, "the mirror drifted from jobs/common/rules.py"
    assert pytest.approx(spark_rules.MAX_RULE_SCORE) == serve.MAX_RULE_SCORE


def test_index_documents_the_endpoints(api):
    """A newbie opens http://localhost:8000/ and must see what to click next."""
    client, _ = api
    body = client.get("/").json()
    assert "service" in body
    assert client.get("/docs").status_code == 200
