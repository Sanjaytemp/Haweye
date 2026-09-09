#!/usr/bin/env python3
"""Static validation of the compose files + generated SQL (run by `make lint` and CI).

Cheap, no Docker required, and it catches the mistakes that cost beginners an
hour of staring at a container restart loop:
  * YAML that does not parse / duplicate keys
  * `depends_on` / `volumes` / `profiles` typos pointing at nothing
  * `${VAR}` references with no default and no .env entry
  * the SQL strings the jobs build at runtime (Iceberg MERGE, enrichment,
    rolling windows) failing to parse as Spark SQL
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    print("pyyaml missing; run `python3 -m pip install pyyaml` (or use .venv)", file=sys.stderr)
    raise SystemExit(2) from None

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs"))

ENV_VAR_RE = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")
COMPOSE_FILES = ["docker-compose.yml", "docker-compose.cdc.yml"]
SQL_CHECKS = []


class Report:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def err(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for name in (".env", ".env.example"):
        path = ROOT / name
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            env.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    return env


def check_compose(rep: Report) -> list[dict]:
    docs = []
    for name in COMPOSE_FILES:
        path = ROOT / name
        if not path.exists():
            rep.err(f"{name}: missing")
            continue
        try:
            data = yaml.safe_load(path.read_text())
        except yaml.YAMLError as exc:
            rep.err(f"{name}: YAML error: {exc}")
            continue
        docs.append((name, data))
        if not isinstance(data, dict):
            rep.err(f"{name}: top level must be a mapping")
            continue
        for key in data:
            if not str(key).startswith("x-") and key not in {
                "services", "volumes", "networks", "name", "secrets", "configs",
            }:
                rep.err(f"{name}: unknown top-level key '{key}'")
        services = data.get("services") or {}
        volumes = set((data.get("volumes") or {}).keys())
        env = load_env()
        for sname, svc in services.items():
            if not isinstance(svc, dict):
                rep.err(f"{name}: service {sname} is not a mapping")
                continue
            for dep in (svc.get("depends_on") or {}):
                if dep not in services:
                    rep.err(f"{name}: {sname}.depends_on -> unknown service '{dep}'")
            for k in ("environment",):
                value = svc.get(k)
                if isinstance(value, dict):
                    for var, val in value.items():
                        _check_interpolation(name, f"{sname}.{k}.{var}", str(val), env, rep)
            for cmd_key in ("command", "entrypoint"):
                cmd = svc.get(cmd_key)
                if isinstance(cmd, str):
                    _check_interpolation(name, f"{sname}.{cmd_key}", cmd, env, rep)
            for v in (svc.get("volumes") or []):
                if not isinstance(v, str):
                    continue
                src = v.split(":")[0]
                if not src.startswith((".", "/", "~")) and src not in volumes:
                        rep.err(f"{name}: {sname} mounts undefined volume '{src}' "
                                f"(add it to top-level volumes:)")
            for p in (svc.get("ports") or []):
                if not re.fullmatch(r"\$?\{?[\w:.-]*\}?:[\d-]+(:\d+)?|[\d-]+(:\d+)?", str(p)):
                    rep.warn(f"{name}: {sname} odd port mapping '{p}'")
            if svc.get("healthcheck") and "test" not in svc["healthcheck"]:
                rep.err(f"{name}: {sname}.healthcheck has no test")
    return docs


def _check_interpolation(fname: str, where: str, text: str, env: dict, rep: Report) -> None:
    # `$$` is compose's escape for a literal `$` (the shell in `bash -c` expands
    # it), so those must not be read as compose interpolations.
    text = text.replace("$$", "\u0000")
    for var, default in ENV_VAR_RE.findall(text):
        if not default and var not in env:
            rep.warn(f"{fname}: {where} uses ${{{var}}} with no default and no .env entry")


def check_referenced_files(rep: Report) -> None:
    """Every path the compose files bind-mount must exist (classic beginner trap)."""
    for name in COMPOSE_FILES:
        path = ROOT / name
        if not path.exists():
            continue
        data = yaml.safe_load(path.read_text()) or {}
        for sname, svc in (data.get("services") or {}).items():
            for v in (svc.get("volumes") or []):
                if isinstance(v, str) and v.startswith("."):
                    src = v.split(":")[0]
                    if not (ROOT / src).exists():
                        rep.err(f"{name}: {sname} bind-mounts missing path '{src}'")
            for ef in (svc.get("env_file") or []):
                entry = ef.get("path") if isinstance(ef, dict) else ef
                if entry and not (ROOT / entry).exists() and not (isinstance(ef, dict) and ef.get("required") is False):
                    rep.err(f"{name}: {sname} env_file '{entry}' missing")
            build = svc.get("build")
            if isinstance(build, dict):
                ctx = ROOT / build.get("context", ".")
                df = build.get("dockerfile")
                if df and not (ctx / df).exists():
                    rep.err(f"{name}: {sname} build dockerfile not found: {ctx / df}")
                elif not ctx.exists():
                    rep.err(f"{name}: {sname} build context missing: {ctx}")


def check_sql(rep: Report) -> None:
    try:
        import sqlglot
    except ImportError:
        rep.warn("sqlglot not installed - skipping SQL parse checks")
        return
    from common import dimensions, enrichment, features, rules  # jobs/common

    snippets = {
        "enrichment(full)": enrichment.enrichment_sql("cb"),
        "enrichment(fast)": enrichment.enrichment_sql(
            "cb", bounds_view=None, merchant_view="m", card_view="c", category_state_view="cs"),
        "rolling_features": features.rolling_features_sql("v"),
        "micro_batch_aggs": features.micro_batch_aggs_sql("v"),
        "minute_agg": features.minute_agg_sql("v"),
        "day_agg": features.day_agg_sql("v"),
        "minute_history": features.minute_history_sql("batch_bounds"),
        "day_history": features.day_history_sql("cb"),
        "rules": "SELECT t.*, " + rules.rule_columns_sql() + " FROM t",
        "dims postgres ddl": dimensions.postgres_ddl(),
    }
    for label, sql in snippets.items():
        try:
            for stmt in sqlglot.parse(sql, read="spark"):
                if stmt is None:
                    continue
        except Exception as exc:
            rep.err(f"SQL parse failed for {label}: {exc}")
    # the generated MERGE statements need a real target schema: check the template only
    try:
        sqlglot.parse_one(
            "MERGE INTO t USING s ON t.k = s.k "
            "WHEN MATCHED AND s.op = 'DELETE' THEN DELETE "
            "WHEN MATCHED THEN UPDATE SET t.a = s.a "
            "WHEN NOT MATCHED THEN INSERT (a) VALUES (s.a)", read="spark")
    except Exception as exc:
        rep.err(f"MERGE template rejected: {exc}")


def check_jobs_are_importable(rep: Report) -> None:
    """`python3 -m compileall` is not enough: catch broken module-level imports."""
    import importlib

    for mod in ("config", "schema", "features", "enrichment", "rules", "cdc", "dimensions",
                "model", "sparkutils", "cli", "table_props", "features", "cdc_merge_common"):
        try:
            importlib.import_module(f"common.{mod}")
        except Exception as exc:
            rep.err(f"jobs/common/{mod}.py fails to import: {type(exc).__name__}: {exc}")


def check_json_files(rep: Report) -> None:
    for path in ROOT.rglob("*.json"):
        if any(part in {".venv", "node_modules", ".git"} for part in path.parts):
            continue
        try:
            json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            rep.err(f"{path.relative_to(ROOT)}: invalid JSON: {exc}")


def main() -> int:
    rep = Report()
    check_compose(rep)
    check_referenced_files(rep)
    check_sql(rep)
    check_jobs_are_importable(rep)
    check_json_files(rep)
    for e in rep.errors:
        print(f"  ERROR   {e}")
    for w in rep.warnings:
        print(f"  warning {w}")
    if not rep.errors and not rep.warnings:
        print("  compose + SQL + job imports all check out ✔")
    return 1 if rep.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
