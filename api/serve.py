"""Serving layer — a small FastAPI app in front of Redis + Postgres + the model.

Why is it separate from Spark?  Because a scorer must answer in single-digit
milliseconds; a Spark job is a *batch of those*, not a request handler.  The
streaming jobs write the state, this service reads it:

    GET  /healthz                 readiness of every dependency
    GET  /v1/score/{txn_id}       decision for a transaction that was scored
    POST /v1/score                score a *hypothetical* transaction live
    GET  /v1/features/{txn_id}    the exact feature vector the model saw
    GET  /v1/card/{card_id}       the card's rolling features (newest first)
    GET  /v1/alerts               the open queue + PATCH to work it
    GET  /v1/model                which model version is live, its metrics
    POST /v1/model/refresh        re-read the joblib artifact (zero downtime)

`POST /v1/score` is the teaching endpoint: it derives the features it needs
from the online store and applies the same rules + model as the streaming job.
"""
from __future__ import annotations

import json
import math
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import redis
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

API_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.environ.get("MODEL_DIR", os.path.join(API_DIR, "..", "artifacts", "models"))
MODEL_POINTER = os.environ.get("MODEL_POINTER_FILE", "version.txt")
REDIS_URL = os.environ.get("REDIS_URL", f"redis://{os.environ.get('REDIS_HOST', 'localhost')}:"
                                    f"{os.environ.get('REDIS_PORT', '6379')}/0")
DB_URI = os.environ.get("SERVING_DB_URI",
                        "postgresql://lakehouse_app:lakehouse_app@localhost:5432/lakehouse")
FEATURE_PREFIX = os.environ.get("REDIS_FEATURE_PREFIX", "feat:txn:")
CARD_PREFIX = os.environ.get("REDIS_CARD_PREFIX", "feat:card:")
SCORE_PREFIX = os.environ.get("REDIS_SCORE_PREFIX", "score:txn:")
ALERT_FLOOR = float(os.environ.get("ALERT_MIN_SCORE", "0.75"))
API_TOKEN = os.environ.get("API_TOKEN", "")

app = FastAPI(title="haweye fraud serving", version="1.0.0",
              description="Real-time fraud scoring + feature access for the "
                          "Haweye lakehouse project")

# ------------------------------------------------------------------ infra bits
_redis = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)
_local = threading.local()


def db():
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            # Ask the backend something cheap: psycopg2 notices a dead connection on
            # the next statement, never on an attribute read, so a "touch" here would
            # happily return a connection that 500s on the first real query.
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return conn
        except Exception:
            pass
    conn = psycopg2.connect(DB_URI)
    conn.autocommit = True
    _local.conn = conn
    return conn


def query(sqltext: str, params: tuple = ()) -> list[dict]:
    try:
        with db().cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sqltext, params)
            return [dict(r) for r in cur.fetchall()]
    except Exception as exc:
        _local.conn = None
        raise HTTPException(502, f"database unavailable: {type(exc).__name__}: {exc}") from exc


# ------------------------------------------------------------------ the model
class ModelHolder:
    """Loads the sklearn bundle once and reloads it when the pointer file changes.

    Zero-downtime deploys: the training job drops new files, `POST
    /v1/model/refresh` (or the pointer's mtime) makes the next request use them.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.bundle: dict[str, Any] | None = None
        self.meta: dict[str, Any] = {}
        self.loaded_at: float = 0.0
        self.pointer_stamp: float | None = None

    def pointer(self) -> tuple[str | None, float | None]:
        path = os.path.join(MODEL_DIR, MODEL_POINTER)
        try:
            stamp = os.path.getmtime(path)
            with open(path) as fh:
                line = fh.read().strip().splitlines()[-1] if os.path.getsize(path) else ""
            version = line.split(",")[0].strip() if line else None
            return (version or None), stamp
        except OSError:
            return None, None

    def ensure(self, force: bool = False) -> dict[str, Any] | None:
        version, stamp = self.pointer()
        with self.lock:
            if not force and self.bundle is not None and stamp == self.pointer_stamp:
                return self.bundle
            try:
                import joblib

                path = os.path.join(MODEL_DIR, "sklearn_model.joblib")
                bundle = joblib.load(path)
                bundle["metadata"]["artifact_path"] = path
                self.bundle = bundle
                self.meta = bundle.get("metadata", {})
                self.loaded_at = time.time()
                self.pointer_stamp = stamp
                print(f">>> model loaded: {version} ({path})", flush=True)
                return self.bundle
            except Exception as exc:
                print(f">>> model not loadable ({type(exc).__name__}: {exc}); rules only", flush=True)
                return None

    def status(self) -> dict[str, Any]:
        version, _stamp = self.pointer()
        return {
            "pointer_version": version,
            "loaded": self.bundle is not None,
            "loaded_at": datetime.fromtimestamp(self.loaded_at, timezone.utc).isoformat()
            if self.loaded_at else None,
            "model_dir": MODEL_DIR,
            "auc": (self.meta.get("metrics") or {}).get("auc"),
            "algorithm": self.meta.get("algorithm"),
            "feature_count": len(self.meta.get("sklearn_feature_order") or []),
        }


MODEL = ModelHolder()


# --------------------------------------------------------------------- rules
# Mirror of jobs/common/rules.py.  If you change one, change both; the unit test
# `tests/unit/test_rules.py` compares them, and that is not optional.
RULES: list[dict[str, Any]] = [
    dict(rule_id="CARD_VELOCITY_5MIN", weight=0.30, why=">= 5 transactions on the card in the last 5 minutes",
         hit=lambda f: _num(f, "txn_count_5min") >= 5),
    dict(rule_id="AMOUNT_SPIKE_1H", weight=0.22, why="more than 1,500 spent in the last hour",
         hit=lambda f: _num(f, "amount_sum_1h") > 1500),
    dict(rule_id="EXTREME_TICKET", weight=0.18, why="ticket above 5,000 or above 90% of the credit limit",
         hit=lambda f: _num(f, "amount") > 5000 or _num(f, "amount_to_limit") > 0.9),
    dict(rule_id="UNUSUAL_FOR_CARD", weight=0.16,
         why="amount wildly outside this card's normal pattern",
         hit=lambda f: (_num(f, "amount_zscore_24h") > 4 and _num(f, "amount") > 200)
         or (_num(f, "amount_to_avg_1h") > 8 and _num(f, "amount") > 150)),
    dict(rule_id="CNP_ABROAD", weight=0.20, why="card-not-present abroad at a high-risk merchant",
         hit=lambda f: _bool(f, "country_mismatch") and not _bool(f, "card_present")
         and _num(f, "merchant_risk_score") >= 0.6),
    dict(rule_id="NEW_CATEGORY_HIGH_AMOUNT", weight=0.14, why="first-ever spend in this category above 300",
         hit=lambda f: _bool(f, "is_new_merchant_category") and _num(f, "amount") > 300),
    dict(rule_id="CLOSED_MERCHANT", weight=0.45, why="merchant is flagged closed/on-blocklist",
         hit=lambda f: _bool(f, "merchant_closed_hit")),
    dict(rule_id="LIMIT_EXCEEDED", weight=0.25, why="amount exceeds the card's 1-hour limit",
         hit=lambda f: _num(f, "ratio_to_hourly_limit") > 1),
]
MAX_RULE_SCORE = sum(r["weight"] for r in RULES)


def _num(features: dict, key: str, default: float = 0.0) -> float:
    v = features.get(key, default)
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _bool(features: dict, key: str) -> bool:
    v = features.get(key)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "t", "yes"}
    return bool(v)


def evaluate_rules(features: dict) -> tuple[list[str], float]:
    hits = [r["rule_id"] for r in RULES if r["hit"](features)]
    score = sum(r["weight"] for r in RULES if r["rule_id"] in hits) / MAX_RULE_SCORE
    return hits, round(score, 4)


def decide(score: float, threshold: float) -> str:
    if score >= ALERT_FLOOR:
        return "decline"
    if score >= threshold:
        return "review"
    if score >= threshold * 0.6:
        return "monitor"
    return "approve"


# ------------------------------------------------------------------- payloads
class TxnIn(BaseModel):
    transaction_id: str | None = None
    card_id: str = Field(..., examples=["CARD-00042"])
    merchant_id: str | None = Field(None, examples=["MER-0007"])
    amount: float = Field(..., gt=0, examples=[1899.9])
    currency: str = "USD"
    channel: str = Field("online", examples=["online", "pos", "atm"])
    merchant_country: str | None = None
    card_present: bool = False
    is_3ds: bool = False
    merchant_category: str | None = None
    event_ts: str | None = None


class ScoreOut(BaseModel):
    transaction_id: str | None
    score: float
    decision: str
    model_score: float | None
    rule_score: float
    rule_hits: list[str]
    features_used: dict[str, Any]
    model_version: str | None
    latency_ms: float


def load_online_features(card_id: str, txn_id: str | None) -> dict[str, Any]:
    """Redis first (ms), Postgres as the fallback (the online store is a cache)."""
    for key in ((f"{FEATURE_PREFIX}{txn_id}",) if txn_id else ()) + (f"{CARD_PREFIX}{card_id}",):
        raw = None
        try:
            raw = _redis.get(key)
        except Exception:
            pass
        if raw:
            try:
                data = json.loads(raw)
                data["__source"] = f"redis:{key}"
                return data
            except json.JSONDecodeError:
                pass
    if txn_id:
        rows = query("SELECT * FROM public.transactions_feature_store WHERE transaction_id = %s", (txn_id,))
        if rows:
            rows[0]["__source"] = "postgres:txn"
            return rows[0]
    rows = query("""SELECT * FROM public.transactions_feature_store
                    WHERE card_id = %s ORDER BY event_ts_ts DESC NULLS LAST LIMIT 1""", (card_id,))
    if rows:
        rows[0]["__source"] = "postgres:card"
        return rows[0]
    return {"__source": "none"}


def derive_features(payload: TxnIn, online: dict[str, Any]) -> dict[str, Any]:
    """The same derivation the streaming job does, on one hypothetical event.

    `prior + current` is the standard "as-of, including this event" convention:
    a card that has made 4 recent payments is on its 5th, so velocity becomes 5.
    """
    f: dict[str, Any] = dict(online)
    amount = float(payload.amount)
    f.update({
        "amount": amount,
        "card_id": payload.card_id,
        "merchant_id": payload.merchant_id,
        "channel": payload.channel,
        "card_present": payload.card_present,
        "is_3ds": payload.is_3ds,
        "merchant_category": payload.merchant_category or f.get("merchant_category"),
    })
    f["txn_count_5min"] = int(_num(f, "txn_count_5min")) + 1
    f["txn_count_1h"] = int(_num(f, "txn_count_1h")) + 1
    f["txn_count_24h"] = int(_num(f, "txn_count_24h")) + 1
    f["amount_sum_5min"] = _num(f, "amount_sum_5min") + amount
    f["amount_sum_1h"] = _num(f, "amount_sum_1h") + amount
    f["amount_sum_24h"] = _num(f, "amount_sum_24h") + amount
    f["amount_max_1h"] = max(_num(f, "amount_max_1h"), amount)
    limit = _num(f, "credit_limit", 0) or 0
    f["amount_to_limit"] = (amount / limit) if limit else None
    avg1h = _num(f, "amount_sum_1h") / max(1, int(_num(f, "txn_count_1h")))
    f["amount_avg_1h"] = avg1h
    f["amount_to_avg_1h"] = (amount / avg1h) if avg1h else None
    f["log_amount"] = math.log(1 + amount)
    std24 = _num(f, "amount_std_24h")
    avg24 = _num(f, "amount_avg_24h")
    f["amount_zscore_24h"] = ((amount - avg24) / std24) if std24 > 0 else None
    hourly = _num(f, "txn_limit_1h")
    f["ratio_to_hourly_limit"] = (amount / hourly) if hourly else None
    ticket = _num(f, "merchant_avg_ticket")
    f["ratio_to_merchant_avg"] = (amount / ticket) if ticket else None
    if payload.merchant_country and f.get("issuer_country"):
        f["country_mismatch"] = payload.merchant_country != f["issuer_country"]
    f.setdefault("is_new_merchant_category", False)
    f.setdefault("merchant_closed_hit", False)
    return f


def vector_from_features(features: dict[str, Any], order: list[str], meta: dict) -> object:
    import numpy as np

    cats = meta.get("categorical_levels") or {}
    vals: list[float] = []
    for name in order:
        if "=" in name:
            col, _, level = name.partition("=")
            vals.append(1.0 if str(features.get(col)) == level else 0.0)
        elif name in cats:
            vals.append(0.0)
        else:
            vals.append(_num(features, name, 0.0))
    return np.asarray([vals], dtype="float64")


# ------------------------------------------------------------------- endpoints
@app.get("/healthz", tags=["ops"])
def healthz() -> JSONResponse:
    checks: dict[str, Any] = {}
    try:
        checks["redis"] = _redis.ping() and "ok"
    except Exception as exc:
        checks["redis"] = f"unavailable: {type(exc).__name__}"
    try:
        checks["postgres"] = query("SELECT 1 AS ok")[0]["ok"] == 1 and "ok"
    except Exception as exc:
        checks["postgres"] = f"unavailable: {exc.__class__.__name__}"
    checks["model"] = MODEL.status()
    try:
        n = query("SELECT count(*) AS c FROM public.fraud_scores")[0]["c"]
        checks["scored_transactions"] = n
    except Exception:
        checks["scored_transactions"] = None
    code = 200 if checks.get("redis") == "ok" or checks.get("postgres") == "ok" else 503
    return JSONResponse(checks, status_code=code)


@app.post("/v1/score", response_model=ScoreOut, tags=["scoring"])
def score_transaction(payload: TxnIn, x_api_token: str | None = Header(default=None)) -> ScoreOut:
    require_token(x_api_token)
    t0 = time.perf_counter()
    online = load_online_features(payload.card_id, payload.transaction_id)
    features = derive_features(payload, online)
    hits, rule_score = evaluate_rules(features)
    threshold = float((MODEL.meta or {}).get("threshold") or 0.6)

    bundle = MODEL.ensure()
    model_score: float | None = None
    if bundle is not None:
        try:
            order = bundle["metadata"].get("sklearn_feature_order") or []
            X = vector_from_features(features, order, bundle["metadata"])
            model_score = float(bundle["model"].predict_proba(X)[0][1])
        except Exception as exc:  # a bad vector must not take the API down
            print(f"!!! model scoring failed: {exc}", flush=True)
    # rules carry the full score while no model is loaded (cold start / model
    # outage) - identical semantics to jobs/common/rules.blend()
    if model_score is None:
        final = max(0.0, min(1.0, rule_score))
    else:
        final = max(0.0, min(1.0, 0.75 * model_score + 0.25 * rule_score))
    txn_id = payload.transaction_id or f"HYP-{datetime.now(timezone.utc):%Y%m%d%H%M%S}"
    persist = os.environ.get("API_PERSIST", "true").lower() in {"1", "true"}
    if persist:
        try:
            query(
                """INSERT INTO public.fraud_scores
                     (transaction_id, card_id, event_ts_ts, amount, model_version, model_score,
                      rule_score, final_score, decision, rule_hits, feature_snapshot, scored_at)
                   VALUES (%s,%s, coalesce(%s::timestamptz, now()), %s,%s,%s,%s,%s,%s,%s,%s::jsonb, now())
                   ON CONFLICT (transaction_id) DO UPDATE SET final_score = EXCLUDED.final_score,
                       decision = EXCLUDED.decision, rule_hits = EXCLUDED.rule_hits""",
                (txn_id, payload.card_id, payload.event_ts, payload.amount,
                 MODEL.meta.get("version"), model_score, rule_score, final,
                 decide(final, threshold), hits,
                 json.dumps({k: v for k, v in features.items() if not k.startswith("__")}, default=str)),
            )
        except Exception as exc:
            print(f">>> could not persist score (continuing): {exc}", flush=True)
    used = {k: features.get(k) for k in
            ("txn_count_5min", "txn_count_1h", "amount_sum_1h", "amount_sum_24h", "amount_to_limit",
             "amount_to_avg_1h", "amount_zscore_24h", "country_mismatch", "credit_limit", "__source")}
    return ScoreOut(
        transaction_id=txn_id, score=round(final, 4), decision=decide(final, threshold),
        model_score=None if model_score is None else round(model_score, 4),
        rule_score=rule_score, rule_hits=hits,
        features_used={k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in used.items()},
        model_version=MODEL.meta.get("version"),
        latency_ms=round((time.perf_counter() - t0) * 1000, 2),
    )


@app.get("/v1/score/{txn_id}", tags=["scoring"])
def get_score(txn_id: str) -> dict:
    rows = query("SELECT * FROM public.fraud_scores WHERE transaction_id = %s", (txn_id,))
    if rows:
        return rows[0]
    try:
        raw = _redis.get(f"{SCORE_PREFIX}{txn_id}")
    except Exception:
        raw = None
    if raw:
        return json.loads(raw)
    raise HTTPException(404, f"no score for {txn_id} (has the pipeline scored it yet?)")


@app.get("/v1/features/{txn_id}", tags=["features"])
def get_features(txn_id: str) -> dict:
    """The exact vector the model was shown - the answer to 'why this score?'."""
    try:
        raw = _redis.get(f"{FEATURE_PREFIX}{txn_id}")
    except Exception:
        raw = None
    if raw:
        return json.loads(raw)
    rows = query("SELECT * FROM public.transactions_feature_store WHERE transaction_id = %s", (txn_id,))
    if rows:
        return rows[0]
    raise HTTPException(404, f"no cached features for {txn_id}")


@app.get("/v1/card/{card_id}", tags=["features"])
def card_features(card_id: str, limit: int = Query(20, le=200)) -> dict:
    rows = query("""SELECT * FROM public.transactions_feature_store
                    WHERE card_id = %s ORDER BY event_ts_ts DESC NULLS LAST LIMIT %s""",
                 (card_id, limit))
    return {"card_id": card_id, "features": rows, "online": json.loads(_redis.get(f"{CARD_PREFIX}{card_id}")
                                                                       or "null") if redis_ok() else None}


def redis_ok() -> bool:
    try:
        return bool(_redis.ping())
    except Exception:
        return False


@app.get("/v1/merchant/{merchant_id}", tags=["reference"])
def merchant(merchant_id: str) -> dict:
    rows = query("""SELECT m.*, (SELECT max(synced_at) FROM public.merchants_lakehouse l
                                   WHERE l.merchant_id = m.merchant_id) AS lakehouse_synced_at
                    FROM public.merchants m WHERE m.merchant_id = %s""", (merchant_id,))
    if not rows:
        raise HTTPException(404, f"unknown merchant {merchant_id}")
    mirror = query("SELECT * FROM public.merchants_lakehouse WHERE merchant_id = %s", (merchant_id,))
    return {"source_of_truth": rows[0], "lakehouse_mirror": mirror[0] if mirror else None,
            "note": "lakehouse_mirror is what the streaming enrichment joined; it is filled by CDC"}


@app.get("/v1/alerts", tags=["ops"])
def alerts(status: str = "open", limit: int = Query(25, le=200)) -> dict:
    rows = query("""SELECT a.*, s.amount, s.model_score, s.rule_score, s.model_version
                    FROM public.fraud_alerts a LEFT JOIN public.fraud_scores s USING (transaction_id)
                    WHERE a.status = %s ORDER BY a.score DESC NULLS LAST, a.opened_at DESC LIMIT %s""",
                 (status, limit))
    return {"status": status, "count": len(rows), "alerts": rows}


@app.patch("/v1/alerts/{alert_id}", tags=["ops"])
def update_alert(alert_id: int, status: str = Query(..., pattern="^(open|investigating|cleared|confirmed)$"),
                 assignee: str | None = None, notes: str | None = None,
                 x_api_token: str | None = Header(default=None)) -> dict:
    require_token(x_api_token)
    query("""UPDATE public.fraud_alerts SET status=%s, assigned_to=coalesce(%s, assigned_to),
             notes=coalesce(%s, notes), closed_at=CASE WHEN %s IN ('cleared','confirmed') THEN now() END
             WHERE alert_id=%s""", (status, assignee, notes, status, alert_id))
    return {"alert_id": alert_id, "status": status}


@app.get("/v1/stats", tags=["ops"])
def stats(window_minutes: int = Query(60, le=10_080)) -> dict:
    rows = query("""SELECT count(*) AS scored,
                           count(*) FILTER (WHERE decision <> 'approve') AS flagged,
                           round(avg(final_score)::numeric, 4)  AS avg_score,
                           max(final_score)                     AS max_score,
                           count(DISTINCT card_id)              AS cards,
                           count(*) FILTER (WHERE model_version IS NULL) AS scored_without_model
                    FROM public.fraud_scores
                    WHERE scored_at > now() - (%s || ' minutes')::interval""", (window_minutes,))
    return {"window_minutes": window_minutes, **(rows[0] if rows else {})}


@app.get("/v1/model", tags=["model"])
def model_info() -> dict:
    meta = dict(MODEL.meta or {})
    meta.pop("feature_importance", None)
    return {"status": MODEL.status(), "metadata": meta,
            "features_expected": (MODEL.meta or {}).get("features", [])[:60]}


@app.post("/v1/model/refresh", tags=["model"])
def model_refresh(x_api_token: str | None = Header(default=None)) -> dict:
    require_token(x_api_token)
    bundle = MODEL.ensure(force=True)
    if bundle is None:
        raise HTTPException(503, f"no usable model under {MODEL_DIR}")
    return {"refreshed": True, **MODEL.status()}


def require_token(token: str | None) -> None:
    if API_TOKEN and token != API_TOKEN:
        raise HTTPException(401, "missing/invalid X-API-Token")


@app.get("/", include_in_schema=False)
def index() -> dict:
    return {
        "service": "haweye fraud serving",
        "docs": "/docs",
        "model": MODEL.status(),
        "endpoints": ["/healthz", "/v1/score", "/v1/score/{txn_id}", "/v1/features/{txn_id}",
                      "/v1/card/{card_id}", "/v1/merchant/{id}", "/v1/alerts", "/v1/stats",
                      "/v1/model", "/v1/model/refresh"],
        "openapi": "/openapi.json",
        "hint": "POST /v1/score first; the streaming jobs fill the same tables from Kafka",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")),
                log_level=os.environ.get("LOG_LEVEL", "info").lower())
