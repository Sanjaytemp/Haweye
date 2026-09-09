#!/usr/bin/env python3
"""Make the dimension tables *change* so CDC has something to capture.

Every ~20s it performs one realistic operational action and prints it, e.g.

    [cdc] UPDATE merchants SET merchant_risk_score=0.91 WHERE merchant_id='MER-0042'
    [cdc] INSERT INTO merchants (merchant_id, ...) VALUES ('MER-0121', ...)
    [cdc] DELETE FROM card_accounts WHERE card_id='CARD-00177'

Then watch it arrive:  spark-cdc log -> `SELECT ... FROM lake.dim.merchants`.

    python simulate_changes.py --once --kind risk_score
    python simulate_changes.py --interval 5 --rounds 20
"""
from __future__ import annotations

import argparse
import os
import random
import time
from datetime import datetime, timezone

import psycopg2

KINDS = ("risk_score", "close", "reopen", "new_merchant", "travel_notice",
         "limit_raise", "delete_merchant", "category_move")


def connect() -> psycopg2.connection:
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "dimensions"),
        user=os.environ.get("PG_USER", "cdc"),
        password=os.environ.get("PG_PASSWORD", "cdc"),
    )


def one_change(conn, kind: str | None = None, rng: random.Random | None = None) -> str:
    rng = rng or random.Random()
    kind = kind or rng.choice(KINDS)
    with conn.cursor() as cur:
        if kind == "risk_score":
            cur.execute("SELECT merchant_id FROM merchants ORDER BY random() LIMIT 1")
            row = cur.fetchone()
            if not row:
                return "no merchants yet"
            score = round(rng.uniform(0.05, 0.95), 3)
            cur.execute("UPDATE merchants SET merchant_risk_score=%s WHERE merchant_id=%s",
                        (score, row[0]))
            return f"UPDATE merchants SET merchant_risk_score={score} WHERE merchant_id='{row[0]}'"
        if kind in ("close", "reopen"):
            cur.execute("SELECT merchant_id FROM merchants WHERE merchant_closed = %s "
                        "ORDER BY random() LIMIT 1", (kind == "reopen",))
            row = cur.fetchone()
            if not row:
                return f"nothing to {kind}"
            closed = kind == "close"
            cur.execute("UPDATE merchants SET merchant_closed=%s WHERE merchant_id=%s", (closed, row[0]))
            return f"UPDATE merchants SET merchant_closed={closed} WHERE merchant_id='{row[0]}'"
        if kind == "new_merchant":
            mid = f"MER-{rng.randrange(900, 999):04d}"
            cur.execute("""INSERT INTO merchants (merchant_id, merchant_name, merchant_category,
                             merchant_country, merchant_risk_score, merchant_avg_ticket, merchant_closed)
                           VALUES (%s, %s, 'crypto', 'JP', 0.85, 900, false)
                           ON CONFLICT (merchant_id) DO NOTHING""",
                        (mid, f"Late Night Crypto {mid[-4:]}"))
            return f"INSERT INTO merchants merchant_id='{mid}' (risk 0.85, crypto, JP)"
        if kind == "travel_notice":
            cur.execute("SELECT card_id FROM card_accounts ORDER BY random() LIMIT 1")
            row = cur.fetchone()
            if not row:
                return "no cards yet"
            cur.execute("UPDATE card_accounts SET travel_notice = NOT travel_notice WHERE card_id=%s",
                        (row[0],))
            cur.execute("SELECT travel_notice FROM card_accounts WHERE card_id=%s", (row[0],))
            return f"UPDATE card_accounts SET travel_notice={cur.fetchone()[0]} WHERE card_id='{row[0]}'"
        if kind == "limit_raise":
            cur.execute("SELECT card_id, credit_limit FROM card_accounts ORDER BY random() LIMIT 1")
            row = cur.fetchone()
            if not row:
                return "no cards yet"
            new_limit = round(float(row[1]) * rng.uniform(1.2, 2.5), 2)
            cur.execute("UPDATE card_accounts SET credit_limit=%s, txn_limit_1h=%s WHERE card_id=%s",
                        (new_limit, round(new_limit * 0.4, 2), row[0]))
            return f"UPDATE card_accounts SET credit_limit={new_limit} WHERE card_id='{row[0]}'"
        if kind == "delete_merchant":
            cur.execute("SELECT merchant_id FROM merchants WHERE merchant_id NOT LIKE 'MER-09%%' "
                        "ORDER BY merchant_id DESC LIMIT 1")
            row = cur.fetchone()
            if not row:
                return "nothing to delete"
            cur.execute("DELETE FROM merchants WHERE merchant_id=%s", (row[0],))
            return f"DELETE FROM merchants WHERE merchant_id='{row[0]}'  (Debezium -> Iceberg DELETE)"
        if kind == "category_move":
            cur.execute("SELECT merchant_id FROM merchants ORDER BY random() LIMIT 1")
            row = cur.fetchone()
            if not row:
                return "no merchants yet"
            cat = rng.choice(["gambling", "wire_transfer", "digital_goods", "electronics"])
            cur.execute("UPDATE merchants SET merchant_category=%s, merchant_avg_ticket="
                        "merchant_avg_ticket * 1.5 WHERE merchant_id=%s", (cat, row[0]))
            return f"UPDATE merchants SET merchant_category='{cat}' WHERE merchant_id='{row[0]}'"
    conn.commit()
    return f"unknown kind {kind}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="haweye CDC change simulator")
    ap.add_argument("--interval", type=float, default=float(os.environ.get("CDC_SIM_INTERVAL", 20)))
    ap.add_argument("--rounds", type=int, default=0, help="0 = forever")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--kind", default=None, choices=list(KINDS))
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args(argv)
    rng = random.Random(args.seed)

    rounds = 1 if args.once else args.rounds
    n = 0
    while True:
        try:
            conn = connect()
            conn.autocommit = True
            break
        except Exception as exc:
            print(f"[cdc] waiting for postgres: {exc}", flush=True)
            time.sleep(3)
    while True:
        n += 1
        print(f"[cdc] {datetime.now(timezone.utc).isoformat(timespec='seconds')} {one_change(conn, args.kind, rng)}",
              flush=True)
        if rounds and n >= rounds:
            break
        time.sleep(args.interval if not args.once else 0)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
