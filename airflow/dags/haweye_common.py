"""Shared Airflow helpers for the haweye DAGs.

Nothing clever here on purpose: a `BashOperator` that calls the same
`./jobs/submit/run_job.sh <job> ...` you would type by hand is easier to debug
than a custom operator, and a failed task can be reproduced in a terminal in one
copy-paste.  In a real team you would swap this for
`apache-airflow-providers-apache-spark`'s `SparkSubmitOperator`; the DAG
structure would not change.

Two Airflow concepts worth knowing (they bite everyone once):

* **logical date (`ds`)** - the date a run *represents*, not the date it ran.
  Templates must use `{{ ds }}`, never `datetime.now()`, or backfills silently
  re-process "today".
* **a new DAG file starts paused** in Airflow 2.x.  `make airflow-unpause`.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

REPO = os.environ.get("HAWEYE_REPO", "/opt/airflow")
METRICS_FILE = os.environ.get("HAWEYE_METRICS_FILE", "/opt/airflow/logs/haweye_latest_metrics.json")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
    "execution_timeout": timedelta(minutes=45),
}


def _shellquote(value: Any) -> str:
    s = str(value)
    if any(c in s for c in " '\"$`&;|<>()"):
        return "'" + s.replace("'", "'\"'\"'") + "'"
    return s


def job_command(job: str, args: list[str] | tuple[str, ...] = (), *, timeout_s: int = 1800) -> str:
    """The one command line every Spark task runs (tail keeps Airflow logs small)."""
    quoted = " ".join(_shellquote(a) for a in args)
    return (
        f"set -o pipefail; cd {REPO}; "
        f"timeout {timeout_s} ./jobs/submit/run_job.sh {job} {quoted} 2>&1 | tail -400"
    )


def spark_bash(task_id: str, job: str, *args: str, **kwargs) -> BashOperator:
    """`BashOperator` wired to `run_job.sh`, with the project's standard retries."""
    timeout_s = kwargs.pop("timeout_s", 1800)
    bash = kwargs.pop("bash_command", None) or job_command(job, list(args), timeout_s=timeout_s)
    return BashOperator(task_id=task_id, bash_command=bash, **kwargs)


def window_args(days: int = 30, holdout_days: int = 5) -> list[str]:
    """Args for the two time-window jobs (`--days` counts back from *now*).

    Kept as a list, not a string, so it composes with other args without
    quoting surprises.
    """
    return ["--days", str(days), "--holdout-days", str(holdout_days)]


def partition_range(days: int = 1) -> list[str]:
    """`--start/--end` for a "the day that just closed" backfill."""
    # Airflow still has to see `{{ ... }}`, hence the doubled braces in the f-string
    return ["--start", f"{{{{ macros.ds_add(ds, -{days}) }}}}", "--end", "{{ ds }}"]


def load_latest_metrics() -> dict:
    try:
        with open(METRICS_FILE) as fh:
            return json.load(fh)
    except Exception:
        return {}


def make_dag(dag_id: str, schedule: str | None, *, start_date=datetime(2024, 1, 1),
             catchup: bool = False, tags: list[str] | None = None, doc_md: str = "",
             **kwargs) -> DAG:
    return DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        max_active_runs=1,             # never two overlapping maintenance runs
        tags=tags or ["haweye"],
        doc_md=doc_md,
        **kwargs,
    )


__all__ = ["TaskGroup", "BashOperator", "DEFAULT_ARGS", "METRICS_FILE", "job_command",
           "load_latest_metrics", "make_dag", "partition_range", "spark_bash", "window_args"]
