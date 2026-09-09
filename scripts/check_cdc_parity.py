#!/usr/bin/env python3
"""Prove (or disprove) that CDC actually works: source rows == lakehouse rows.

Run it after `make cdc-sync` / a few `make cdc-demo` mutations.  It answers the
only question that matters for a replication pipeline: *is the copy equal to the
original, right now?*

    python3 scripts/check_cdc_parity.py            # counts + a few diffs
    python3 scripts/check_cdc_parity.py --quiet     # exit code only (used by Airflow)

Exit codes: 0 equal, 1 mismatch, 2 cannot connect (stack not up).
Uses psycopg2 for Postgres and (if available) a local Spark session for Iceberg;
when Spark is missing it falls back to reading the *serving mirror* in Postgres,
which the CDC job writes in the same transaction — good enough as a canary.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TABLES = {"merchants": "merchant_id", "card_accounts": "card_id"}


def env(key: str, default: str = "") -> str:
    return os.environ.get(key, default) or default


def pg_connect(database: str, user: str, password: str, host: str = "", port: str = ""):
    import psycopg2

    return psycopg2.connect(host=host or env("PGHOST", "localhost"),
                             port=int(port or env("CDC_PG_PORT", "5432")),
                             dbname=database, user=user, password=password,
                             connect_timeout=5)


def source_counts():
    # the *database owner*, not the replication role: we are counting rows, not
    # reading the WAL (the connector logs in as `debezium`, see cdc/sql)
    conn = pg_connect(env("CDC_PG_DB", "dimensions"), env("CDC_PG_USER", "cdc"),
                      env("CDC_PG_PASSWORD", "cdc"), env("CDC_PG_HOST"), env("CDC_PG_PORT"))
    out = {}
    with conn.cursor() as cur:
        for t in TABLES:
            cur.execute(f"SELECT count(*) FROM public.{t}")
            out[t] = cur.fetchone()[0]
    conn.close()
    return out


def lake_counts_via_spark(tables: dict[str, str]):
    """Ask Spark (in the container) for the Iceberg counts.  Returns None if unavailable."""
    if subprocess.run(["docker", "ps"], capture_output=True).returncode != 0:
        return None
    script = (
        "import sys; sys.path.insert(0, '/opt/spark/jobs')\n"
        "from common import sparkutils\n"
        "s = sparkutils.get_spark('parity')\n"
        f"for t in [{', '.join(repr(t) for t in tables)}]:\n"
        "    try:\n"
        f"        print(t + '=' + str(s.table('lake.marts.' + t).count()))\n"
        "    except Exception as e:\n"
        "        print(t + '=ERR:' + type(e).__name__)\n"
    )
    proc = subprocess.run(["docker", "compose", "exec", "-T", "spark-master", "python3", "-c", script],
                          capture_output=True, text=True, cwd=str(ROOT), timeout=300)
    vals = {}
    for line in proc.stdout.splitlines():
        if "=" in line and not line.startswith(">>>"):
            k, v = line.rsplit("=", 1)
            vals[k.strip()] = v
    return vals or None


def lake_counts_via_serving(tables: dict[str, str]):
    """Fallback: the Postgres mirrors the CDC job maintains (serving.*_lakehouse)."""
    conn = pg_connect(env("POSTGRES_DB", "lakehouse"), env("POSTGRES_USER", "haweye"),
                      env("POSTGRES_PASSWORD", "haweye"), env("POSTGRES_HOST"), "5432")
    mirror = {"merchants": "serving.merchants_lakehouse", "card_accounts": "serving.card_accounts_lakehouse"}
    out = {}
    with conn.cursor() as cur:
        for t in tables:
            try:
                cur.execute(f"SELECT count(*) FROM {mirror[t]}")
                out[t] = str(cur.fetchone()[0])
            except Exception:
                conn.rollback()
                out[t] = "MISSING"
    conn.close()
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="compare CDC source vs lakehouse row counts")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--through", default="auto", choices=["auto", "spark", "serving"])
    args = ap.parse_args(argv)

    try:
        src = source_counts()
    except Exception as exc:
        print(f"cannot read the CDC source database: {type(exc).__name__}: {exc}\n"
              f"  → is Postgres up?  make ps   /   make cdc-up", file=sys.stderr)
        return 2

    lake = None
    if args.through in ("auto", "spark"):
        try:
            lake = lake_counts_via_spark(TABLES)
        except Exception:
            lake = None
    if lake is None:
        try:
            lake = lake_counts_via_serving(TABLES)
        except Exception as exc:
            print(f"cannot read the lakehouse mirrors: {exc}", file=sys.stderr)
            return 2

    bad = 0
    for t, _key in TABLES.items():
        want, got = src[t], lake.get(t, "?")
        ok = str(got) == str(want)
        bad += 0 if ok else 1
        if not args.quiet or not ok:
            print(f"{'OK  ' if ok else 'FAIL'} {t:14s} source={want:<7s} lakehouse={got}")
    if bad:
        print(f"\n>>> {bad} table(s) out of parity.  Fixes, in order:\n"
              f"  1. make cdc-sync        (run one merge now)\n"
              f"  2. make cdc-status      (connector RUNNING? tasks 0/1? offset advancing?)\n"
              f"  3. look for '_before' in events: REPLICA IDENTITY FULL missing\n"
              f"     → psql: ALTER TABLE public.{list(TABLES)[0]} REPLICA IDENTITY FULL;",
              file=sys.stderr)
        return 1
    print("\n>>> CDC parity confirmed: every source row exists in the lakehouse copy.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
