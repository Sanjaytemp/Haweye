"""Shared test fixtures.

Three rules this file enforces, because they are the ones that make a test suite
usable rather than decorative:

1. **No docker, no java, no network for the default run.**  `make test` must
   work on a laptop with nothing running, in seconds.  Anything needing the
   stack is marked `docker` or `spark`.
2. **`jobs/` is importable as `common.*`.**  Inside the containers this happens
   via PYTHONPATH; here we add the path explicitly so `pytest` works standalone.
3. **Fixtures generate, not goldens.**  A golden file you cannot read is a
   maintenance trap; a seeded generator you can reproduce is not.
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
# `jobs` and `api` are added as *flat* module dirs (that mirrors how the
# containers run them); `generator` is NOT, because it is a real package here and
# putting it on the path would shadow the package with generator/generator.py.
for candidate in (ROOT / "jobs", ROOT / "api", ROOT):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

os.environ.setdefault("LAKEHOUSE_S3_BUCKET", "lakehouse")
os.environ.setdefault("POSTGRES_HOST", "localhost")


def java_available() -> bool:
    return shutil.which("java") is not None


def port_open(host: str = "localhost", port: int = 9094) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def stack_running() -> bool:
    """Is `docker compose ps` showing a healthy kafka?  (only used by @docker)"""
    if shutil.which("docker") is None:
        return False
    try:
        out = subprocess.run(["docker", "ps", "--format", "{{.Names}}"],
                             capture_output=True, text=True, timeout=10).stdout
    except Exception:
        return False
    return "haweye-kafka" in out


# --------------------------------------------------------------------- fixtures
@pytest.fixture(scope="session")
def rng_seed() -> int:
    return 20240601          # deterministic: a failing test must be reproducible


@pytest.fixture(scope="session")
def sample_txns(rng_seed):
    """~600 transactions from the real simulator, reused by every fast test."""
    from generator import TransactionSimulator, build_cards, build_merchants

    rng = __import__("random").Random(rng_seed)
    merchants = build_merchants(60, rng)
    cards = build_cards(120, rng)
    sim = TransactionSimulator(merchants, cards, rng=rng, fraud_rate=0.25)
    return [sim.one() for _ in range(600)]


@pytest.fixture(scope="session")
def sample_df(sample_txns):
    import pandas as pd

    return pd.DataFrame(sample_txns)


@pytest.fixture
def clean_env(monkeypatch):
    """Isolate env-var-driven config (used by tests that touch `config`)."""
    import importlib

    def _reload(**overrides):
        for k, v in overrides.items():
            monkeypatch.setenv(k, v)
        import common.config as cfg
        importlib.reload(cfg)
        return cfg
    yield _reload
    for name in [m for m in list(sys.modules) if m.startswith("common.")]:
        del sys.modules[name]


def requires_spark(fn):
    return pytest.mark.spark(fn)


pytestmark_collect_ignore = [
    pytest.param("", marks=pytest.mark.skipif(not java_available(), reason="java missing")),
]


def pytest_collection_modifyitems(config, items):
    """Auto-skip spark tests when there is no JVM, docker tests when no stack.

    Skipping (not failing) is deliberate: CI runs the fast suite on every push and
    the heavy suite nightly, and a red X on a laptop for "no java" teaches nothing.
    """
    skip_spark = pytest.mark.skip(reason="java/pyspark not available in this environment")
    skip_docker = pytest.mark.skip(reason="compose stack is not running")
    for item in items:
        if "spark" in item.keywords and not java_available():
            item.add_marker(skip_spark)
        if "docker" in item.keywords and not stack_running():
            item.add_marker(skip_docker)
