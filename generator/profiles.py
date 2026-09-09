"""Dimension profiles (merchants / cards) shared by the generator and the seeder.

Reading them from Postgres when the DB is reachable is what guarantees that the
transactions you stream reference *the same* merchant ids and limits the
enrichment join will look up.  Without this, you get a stream full of
"unknown merchant" rows and every amount-ratio feature goes NULL - a mistake
that costs a beginner an evening.
"""
from __future__ import annotations

import os
import random
from collections.abc import Sequence

try:                       # imported as the `generator` package (tests, IDE)
    from .simulator import build_cards, build_merchants
except ImportError:        # run as a flat script inside the container (/work)
    from simulator import build_cards, build_merchants

DEFAULT_N_MERCHANTS = int(os.environ.get("GEN_MERCHANTS", "120"))
DEFAULT_N_CARDS = int(os.environ.get("GEN_CARDS", "200"))

#: The dimension columns this module reads, in `SELECT` order.  The generator
#: container mounts only `generator/`, so it cannot import the authoritative spec in
#: `jobs/common/dimensions.py`; `tests/unit/test_generator_contract.py` fails if the
#: two lists ever drift, which is the compromise.
MERCHANT_COLUMNS = ["merchant_id", "merchant_name", "merchant_category", "merchant_country",
                    "merchant_risk_score", "merchant_avg_ticket", "merchant_first_seen", "merchant_closed"]
CARD_COLUMNS = ["card_id", "customer_id", "issuer_country", "credit_limit", "txn_limit_1h",
                "travel_notice", "card_age_days", "customer_segment", "card_status"]


def _pg_params() -> dict | None:
    host = os.environ.get("PG_HOST")
    if not host:
        return None
    return {
        "host": host,
        "port": int(os.environ.get("PG_PORT", "5432")),
        "dbname": os.environ.get("PG_DB", "lakehouse"),
        "user": os.environ.get("PG_USER", "haweye"),
        "password": os.environ.get("PG_PASSWORD", "haweye"),
        "connect_timeout": 3,
    }


def load_profiles(seed: int = 1, n_merchants: int | None = None,
                  n_cards: int | None = None, prefer_db: bool = True) -> tuple[list[dict], list[dict], str]:
    """-> (merchants, cards, source) where source is 'postgres' or 'synthetic'."""
    params = _pg_params() if prefer_db else None
    if params:
        try:
            import psycopg2

            with psycopg2.connect(**params) as conn, conn.cursor() as cur:
                cur.execute("SELECT " + ", ".join(MERCHANT_COLUMNS) + " FROM public.merchants")
                merchants = [dict(zip(MERCHANT_COLUMNS, r, strict=True)) for r in cur.fetchall()]
                cur.execute("SELECT " + ", ".join(CARD_COLUMNS) + " FROM public.card_accounts")
                cards = [dict(zip(CARD_COLUMNS, r, strict=True)) for r in cur.fetchall()]
            if len(merchants) >= 5 and len(cards) >= 5:
                return merchants, cards, "postgres"
        except Exception as exc:  # pragma: no cover - depends on env
            print(f">>> profiles: falling back to synthetic ({type(exc).__name__}: {exc})", flush=True)
    merchants = build_merchants(n_merchants or DEFAULT_N_MERCHANTS, random.Random(seed + 1))
    cards = build_cards(n_cards or DEFAULT_N_CARDS, random.Random(seed + 2))
    return merchants, cards, "synthetic"


def pick_subset(rows: Sequence[dict], n: int, rng: random.Random) -> list[dict]:
    if n <= 0 or n >= len(rows):
        return list(rows)
    return rng.sample(list(rows), n)
