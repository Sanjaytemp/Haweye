#!/usr/bin/env python3
"""Load the dimension tables in Postgres (and optionally their history).

    python seed_dimensions.py --merchants 120 --cards 200      # dims only
    python seed_dimensions.py --with-history 7                 # + labelled history
    python seed_dimensions.py --reset                          # drop + recreate

Idempotent (ON CONFLICT DO NOTHING/UPDATE), so you can run it any time.  The
dimension rows are what the *streaming* jobs broadcast-join against, and what
Debezium captures once CDC is switched on - so they must exist before `make
data-backfill`.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import psycopg2  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402

try:
    from .profiles import load_profiles
    from .simulator import generate_transactions
except ImportError:
    from profiles import load_profiles  # noqa: E402
    from simulator import generate_transactions  # noqa: E402

DDL_FILE = os.environ.get("SQL_DIR", os.path.join(os.path.dirname(__file__), "..", "sql"))


def dsn() -> str:
    return (f"host={os.environ.get('PG_HOST', 'localhost')} "
            f"port={os.environ.get('PG_PORT', '5432')} "
            f"dbname={os.environ.get('PG_DB', 'lakehouse')} "
            f"user={os.environ.get('PG_USER', 'haweye')} "
            f"password={os.environ.get('PG_PASSWORD', 'haweye')}")


def apply_ddl(conn) -> None:
    path = os.path.join(DDL_FILE, "10_dimensions.sql")
    with open(path) as fh:
        conn.cursor().execute(fh.read())


def insert_merchants(conn, rows: list[dict]) -> None:
    cols = ["merchant_id", "merchant_name", "merchant_category", "merchant_country",
            "merchant_risk_score", "merchant_avg_ticket", "merchant_first_seen", "merchant_closed"]
    with conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO public.merchants ({", ".join(cols)}) VALUES %s
            ON CONFLICT (merchant_id) DO UPDATE SET
              merchant_name = EXCLUDED.merchant_name,
              merchant_category = EXCLUDED.merchant_category,
              merchant_country = EXCLUDED.merchant_country,
              merchant_risk_score = EXCLUDED.merchant_risk_score,
              merchant_avg_ticket = EXCLUDED.merchant_avg_ticket,
              merchant_closed = EXCLUDED.merchant_closed
        """, [tuple(r[c] for c in cols) for r in rows])


def insert_cards(conn, rows: list[dict]) -> None:
    cols = ["card_id", "customer_id", "issuer_country", "credit_limit", "txn_limit_1h",
            "travel_notice", "card_age_days", "customer_segment", "card_status"]
    with conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO public.card_accounts ({", ".join(cols)}) VALUES %s
            ON CONFLICT (card_id) DO UPDATE SET
              credit_limit = EXCLUDED.credit_limit,
              txn_limit_1h = EXCLUDED.txn_limit_1h,
              travel_notice = EXCLUDED.travel_notice,
              card_age_days = EXCLUDED.card_age_days,
              customer_segment = EXCLUDED.customer_segment,
              card_status = EXCLUDED.card_status
        """, [tuple(r[c] for c in cols) for r in rows])


def insert_labels(conn, labels: list[dict]) -> None:
    rows = [(row["transaction_id"], int(row["label"]), row["fraud_type"]) for row in labels]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO public.transaction_labels (transaction_id, label, fraud_type, source)
            VALUES %s ON CONFLICT (transaction_id) DO NOTHING
        """, [r + ("seed",) for r in rows])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="seed Postgres dimensions (+ history)")
    ap.add_argument("--merchants", type=int, default=120)
    ap.add_argument("--cards", type=int, default=200)
    ap.add_argument("--seed", type=int, default=int(os.environ.get("GEN_SEED", "20240601")))
    ap.add_argument("--with-history", type=int, default=0, help="N days of labelled history in Postgres")
    ap.add_argument("--history-per-day", type=int, default=5000)
    ap.add_argument("--reset", action="store_true", help="truncate the tables first")
    ap.add_argument("--database", default=None, help="override PG_DB (e.g. 'dimensions' for CDC)")
    args = ap.parse_args(argv)

    target = dsn()
    if args.database:
        target = target.replace(f"dbname={os.environ.get('PG_DB', 'lakehouse')}", f"dbname={args.database}")
    print(f">>> connecting: {target.split('password')[0]}...", flush=True)
    conn = psycopg2.connect(target)
    conn.autocommit = False
    apply_ddl(conn)
    if args.reset:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE public.merchants, public.card_accounts, public.transaction_labels CASCADE")
    merchants, cards, source = load_profiles(args.seed, args.merchants, args.cards, prefer_db=False)
    print(f">>> profiles: {len(merchants)} merchants / {len(cards)} cards ({source})", flush=True)
    insert_merchants(conn, merchants)
    insert_cards(conn, cards)
    if args.with_history:
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        txns, labels = generate_transactions(
            start=now - timedelta(days=args.with_history), end=now,
            n=args.with_history * args.history_per_day, seed=args.seed,
        )
        insert_labels(conn, labels)
        print(f">>> wrote {len(txns)} transactions / {sum(row['label'] for row in labels)} labelled fraud",
              flush=True)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM public.merchants")
        nm = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM public.card_accounts")
        nc = cur.fetchone()[0]
    print(f">>> dimensions ready: {nm} merchants, {nc} cards", flush=True)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
