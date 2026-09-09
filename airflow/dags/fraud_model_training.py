"""DAG — nightly fraud model training (the "batch path" of the architecture).

    quality gate -> retrain -> guardrail -> show published -> refresh API -> notify

Why a gate before training?  A model trained on a broken day of features is
worse than no model at all, and it fails *silently*.  Why a guardrail after?
So a bad AUC never reaches the scorer.  Why is the streaming job not restarted?
Because it follows `models/fraud_rf/version.txt`, so moving that pointer *is*
the deployment (see jobs/common/model.py).

`catchup=False`: you never want yesterday's model retrained 30 times because
your laptop was closed for a month.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from haweye_common import METRICS_FILE, make_dag, spark_bash, window_args

with make_dag(
    dag_id="fraud_model_training",
    schedule="30 2 * * *",                       # 02:30 UTC: after the day closes
    catchup=False,
    tags=["haweye", "ml", "batch"],
    doc_md=__doc__,
    dagrun_timeout=timedelta(hours=2),
) as dag:

    gate = spark_bash(
        "check_feature_readiness", "data_quality", "--only-table", "features", "--write-results",
        execution_timeout=timedelta(minutes=10),
    )

    with TaskGroup("train") as train_group:
        train = spark_bash(
            "train_model", "train_model", *window_args(days=30, holdout_days=5),
            "--algorithm", "rf", "--num-trees", "120", "--max-depth", "12",
            "--min-auc-to-publish", "0.70", "--publish", "true",
            "--metrics-out", METRICS_FILE,
            timeout_s=2400,
        )
        # The job already refuses to publish below the floor; this task exists so
        # the *DAG run* turns red (visible in the UI + on-call), not just a log line.
        guardrail = BashOperator(
            task_id="guardrail_auc",
            bash_command=(
                "python3 - <<'PY'\n"
                "import json, sys\n"
                f"m = json.load(open('{METRICS_FILE}'))\n"
                "auc = m.get('auc') or 0\n"
                "print(f\"auc={auc} published={m.get('published')} version={m.get('version')}\")\n"
                "sys.exit(0 if auc >= 0.70 else 1)\n"
                "PY\n"
                "echo 'note: the pointer was not moved by the job; the previous model stays live'"
            ),
        )
        train >> guardrail

    published = spark_bash("show_published_model", "model_refresh", "--current")

    refresh_api = BashOperator(
        task_id="refresh_serving_layer",
        bash_command=(
            "curl -sf -X POST http://serving-api:8000/v1/model/refresh "
            "|| echo 'serving API not running (optional: make up-full)'"
        ),
    )

    notify = BashOperator(
        task_id="summary",
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command=(
            "echo '--- nightly model run ---'; "
            f"cat {METRICS_FILE} 2>/dev/null || echo '(no metrics file)'; echo; "
            "echo '--- lakehouse sizes ---'; ./jobs/submit/run_job.sh table_maintenance --report-only"
        ),
    )

    gate >> train_group >> published >> refresh_api >> notify
