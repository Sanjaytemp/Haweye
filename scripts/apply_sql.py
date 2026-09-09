#!/usr/bin/env python3
"""Apply the repo's SQL to a database, in filename order.  Idempotent-ish by design.

    python3 scripts/apply_sql.py --database lakehouse            # host, psycopg2
    python3 scripts/apply_sql.py --database lakehouse --in-container
        (uses `docker compose exec -T postgres psql`, no local psycopg2 needed)

Postgres' init scripts only run on a *fresh* volume, so after `make nuke` you do
not need this - but if you edit sql/*.sql and want to apply just the delta
without wiping data, this is the tool.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def files(glob: str = "*.sql") -> list[Path]:
    return sorted((ROOT / "sql").glob(glob))


def apply_local(database: str, paths: list[Path], dry: bool) -> int:
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=database,
        user=os.environ.get("POSTGRES_USER", "haweye"),
        password=os.environ.get("POSTGRES_PASSWORD", "haweye"),
    )
    conn.autocommit = True
    for path in paths:
        sql = path.read_text()
        print(f">>> {path.name} ({len(sql)} bytes) -> {database}")
        if dry:
            continue
        with conn.cursor() as cur:
            try:
                cur.execute(sql)
            except Exception as exc:
                print(f"!!! {path.name}: {exc}", file=sys.stderr)
                return 1
    return 0


def apply_in_container(database: str, paths: list[Path], compose: str, dry: bool) -> int:
    for path in paths:
        print(f">>> {path.name} -> {database} (via {compose})")
        if dry:
            continue
        cmd = f"{compose} exec -T -e PSQL_DB={database} postgres " \
              f"psql -v ON_ERROR_STOP=1 -U \"${{POSTGRES_USER:-haweye}}\" -d {database}"
        with open(path) as fh:
            proc = subprocess.run(cmd, shell=True, stdin=fh, capture_output=True, text=True)
        if proc.returncode != 0:
            print(proc.stdout[-2000:], proc.stderr[-2000:], file=sys.stderr)
            return 1
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="apply ./sql to a postgres database")
    ap.add_argument("--database", default=os.environ.get("POSTGRES_DB", "lakehouse"))
    ap.add_argument("--only", default="*.sql")
    ap.add_argument("--in-container", action="store_true", help="pipe into postgres via docker compose")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--compose", default=os.environ.get("COMPOSE", "docker compose"))
    args = ap.parse_args(argv)
    paths = files(args.only)
    if not paths:
        print("no sql files matched", file=sys.stderr)
        return 2
    if args.in_container:
        return apply_in_container(args.database, paths, args.compose, args.dry_run)
    try:
        return apply_local(args.database, paths, args.dry_run)
    except ImportError:
        print("psycopg2 missing -> use --in-container (or: pip install psycopg2-binary)",
              file=sys.stderr)
        return apply_in_container(args.database, paths, args.compose, args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
