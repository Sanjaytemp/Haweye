"""Shared command-line surface for every job, so all entry points behave alike.

    --print-config   show what the job would connect to and exit (no Spark)
    --dry-run        compute, but write nothing
    --sql-only       print the generated SQL and exit (great for learning/inspection)
    --once           process bounded input instead of staying alive
    --starting X     latest | earliest | <json offsets>
"""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable

from . import config


def build_parser(job_name: str, extra: Iterable[tuple] = ()) -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog=job_name, description=f"haweye job: {job_name}")
    ap.add_argument("--print-config", action="store_true", help="dump resolved config and exit")
    ap.add_argument("--dry-run", action="store_true", help="compute but persist nothing")
    ap.add_argument("--once", action="store_true", help="bounded run (streaming job processes then exits)")
    ap.add_argument("--starting", default="latest", help="latest | earliest | json offsets")
    ap.add_argument("--trigger-seconds", type=int, default=config.STREAM_TRIGGER_SECONDS)
    ap.add_argument("--max-rows-per-trigger", type=int, default=config.MAX_ROWS_PER_TRIGGER)
    ap.add_argument("--verbose", action="store_true", help="INFO log level + extra prints")
    for args, kwargs in extra:
        ap.add_argument(*args, **kwargs)
    return ap


def maybe_print_config(args: argparse.Namespace) -> bool:
    if getattr(args, "print_config", False):
        json.dump(config.describe(), sys.stdout, indent=2, default=str)
        print()
        return True
    return False


def spark_log_level(args: argparse.Namespace) -> str:
    return "INFO" if getattr(args, "verbose", False) else os_warn()


def os_warn() -> str:
    import os

    return os.environ.get("SPARK_LOG_LEVEL", "WARN")


def announce(job: str, args: argparse.Namespace) -> None:
    print(f"=== {job} | dry_run={args.dry_run} once={args.once} "
          f"trigger={args.trigger_seconds}s | {config.ICEBERG_CATALOG} @ {config.WAREHOUSE}", flush=True)


def parse_offsets(raw: str) -> str:
    """Validate a `--starting '{"raw_transactions": {"0": 0}}'`-style offset spec."""
    if raw in {"latest", "earliest"}:
        return raw
    try:
        json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"--starting must be latest|earliest|valid JSON, got error: {exc}") from exc
    return raw
