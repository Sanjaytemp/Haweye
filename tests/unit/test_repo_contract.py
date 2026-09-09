"""Tests about the *repository*: compose, env, Makefile, docs and licence.

These look pedantic until the day a renamed env var makes a container start
silently with the wrong defaults.  Each check is cheap and pays for itself the
first time it prevents a "why is the table empty?" debugging session.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def compose(name: str = "docker-compose.yml") -> dict:
    return yaml.safe_load((ROOT / name).read_text())


def test_env_example_covers_every_required_compose_variable():
    """`${VAR}` with no `:-default` MUST be documented, or a fresh clone starts
    with an empty value and a confusing error far away from the cause."""
    documented = set(re.findall(r"^([A-Z0-9_]+)=", (ROOT / ".env.example").read_text(), re.M))
    missing: list[str] = []
    for file in ("docker-compose.yml", "docker-compose.cdc.yml"):
        for var, default in re.findall(r"\$\{([A-Z0-9_]+)(:?-[^}]*)?\}", (ROOT / file).read_text()):
            if not default and var not in documented:
                missing.append(f"{file}: {var}")
    assert not missing, f"undocumented required vars: {sorted(set(missing))}"


def test_no_floating_image_tags():
    """`:latest` in a teaching repo is how "it worked yesterday" happens."""
    offenders = []
    for file in ("docker-compose.yml", "docker-compose.cdc.yml"):
        for svc, spec in (compose(file).get("services") or {}).items():
            img = spec.get("image") or ""
            tag = img.split("/")[-1]
            if img and (tag.endswith(":latest") or ":" not in tag):
                offenders.append(f"{file}:{svc} -> {img}")
    assert not offenders, offenders


def test_long_running_services_declare_a_healthcheck():
    """`make up` uses `--wait`; a service without a healthcheck makes that a no-op."""
    for file in ("docker-compose.yml", "docker-compose.cdc.yml"):
        for svc, spec in (compose(file).get("services") or {}).items():
            if str(spec.get("restart", "")) == "no":          # one-shot initialiser
                continue
            if spec.get("profiles") and "one-shot" in str(spec["profiles"]):
                continue
            cmd = str(spec.get("command", ""))
            if "--if-not-exists" in cmd or "init" in svc or svc in ("generator", "nifi", "mlflow"):
                continue
            assert "healthcheck" in spec, f"{file}:{svc} has no healthcheck"


def test_profiles_are_documented_in_readme():
    text = (ROOT / "README.md").read_text().lower() if (ROOT / "README.md").exists() else ""
    used: set[str] = set()
    for file in ("docker-compose.yml", "docker-compose.cdc.yml"):
        for spec in (compose(file).get("services") or {}).values():
            used.update(spec.get("profiles") or [])
    for prof in sorted(used - {"full"}):
        assert prof in text, f"compose profile '{prof}' is never explained in the README"


def test_makefile_targets_reference_existing_scripts():
    mk = (ROOT / "Makefile").read_text()
    targets = set(re.findall(r"^([a-z0-9_.-]+):", mk, re.M))
    assert {"help", "bootstrap", "up", "down", "nuke", "test", "lint", "cdc-up",
            "jobs-up", "train-model"} <= targets, targets
    for script in set(re.findall(r"\./([A-Za-z0-9_./-]+\.(?:sh|py))", mk)):
        assert (ROOT / script).exists(), f"Makefile calls a missing script: {script}"


def test_every_make_target_is_in_the_help_list():
    """`make help` is generated from `## comments`, so a target without one is
    invisible - which for a beginner equals "does not exist"."""
    mk = (ROOT / "Makefile").read_text()
    targets = list(re.findall(r"^([a-z0-9_.-]+):(.*)$", mk, re.M))
    undocumented = [name for name, rest in targets if "##" not in rest and name != ".DEFAULT_GOAL"]
    assert not undocumented, f"targets missing `## help text`: {undocumented}"


def test_dag_files_are_parseable_and_wired():
    """Airflow itself is not installed here, so we parse: catches syntax errors and
    typos in task wiring, which are the two failure modes of a copy-pasted DAG."""
    import ast

    dags = sorted((ROOT / "airflow" / "dags").glob("*.py"))
    assert len(dags) >= 5, f"expected the documented DAGs, found {dags}"
    for path in dags:
        src = path.read_text()
        ast.parse(src, filename=str(path))
        if path.name == "haweye_common.py":
            continue
        assert "make_dag(" in src, f"{path.name} builds no DAG"
        assert "doc_md=__doc__" in src, f"{path.name} should surface its explanation in the UI"
        assert "catchup=False" in src, f"{path.name} must not backfill by default"


def test_sql_files_are_ordered_and_idempotent():
    files = sorted(p.name for p in (ROOT / "sql").glob("*.sql"))
    assert files == ["00_roles_and_databases.sql", "10_dimensions.sql",
                     "20_serving.sql", "30_catalog.sql"], files
    for path in sorted((ROOT / "sql").glob("*.sql")):
        text = path.read_text()
        assert ("IF NOT EXISTS" in text or "gexec" in text or "GRANT" in text), \
            f"{path.name} must be safe to run twice"


def test_gitignore_keeps_local_state_out():
    ig = (ROOT / ".gitignore").read_text()
    for pattern in ("artifacts", ".env", "__pycache__", ".spark-jars", ".run", ".venv"):
        assert pattern in ig, f"{pattern}/ should be ignored"


def test_licence_readme_and_docs_exist():
    assert (ROOT / "LICENSE").exists(), "a repo without a licence is 'all rights reserved'"
    readme = (ROOT / "README.md").read_text()
    assert len(readme) > 3000, "README is still a stub"
    for section in ("quick start", "architecture", "troubleshooting"):
        assert section in readme.lower(), f"README missing a '{section}' section"


def test_beginner_docs_cover_the_promised_topics():
    docs = {p.name: p.read_text() for p in (ROOT / "docs").glob("*.md")}
    assert len(docs) >= 6, f"docs/ should hold the guided tour, found {sorted(docs)}"
    guide = docs.get("BEGINNER_GUIDE.md") or docs.get("00-beginner-tour.md") or ""
    for topic in ("git", "docker", "kafka", "spark", "iceberg", "airflow", "debezium"):
        assert topic in guide.lower(), f"the beginner guide never explains {topic}"


def test_docs_referenced_from_code_exist():
    """Docstrings cite docs/NN-*.md; a dangling reference is a broken promise."""
    docs = {p.name for p in (ROOT / "docs").glob("*.md")}
    pat = re.compile(r"docs/([0-9A-Za-z._-]+\.md)")
    broken: set[str] = set()
    for folder in ("jobs", "generator", "api", "cdc", "scripts", "infra", "sql", "nifi"):
        for path in (ROOT / folder).rglob("*.py"):
            for name in pat.findall(path.read_text()):
                if name not in docs:
                    broken.add(f"{path.name} -> docs/{name}")
    for path in (ROOT / "cdc").glob("*.sh"):
        for name in pat.findall(path.read_text()):
            if name not in docs:
                broken.add(f"{path.name} -> docs/{name}")
    assert not broken, f"cited docs missing: {sorted(broken)}"


def test_ci_runs_the_same_gates_as_the_makefile():
    ci = ROOT / ".github" / "workflows" / "ci.yml"
    assert ci.exists(), "no CI workflow"
    text = ci.read_text()
    for step in ("make lint", "make test", "ruff", "pytest"):
        assert step in text, f"CI should run `{step}`"
    assert "actions/setup-java" in text, "the spark-marked tests need a JVM in CI"
