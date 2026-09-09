"""DAG — data-quality checks every 15 minutes (streaming pipelines fail quietly).

In a batch pipeline a broken day shows up as "no report this morning".  In a
streaming pipeline nothing announces itself: Kafka keeps accepting writes, the
checkpoint keeps advancing, features silently go NULL for six hours and you find
out from a dashboard on Friday.  This DAG is the thing that screams instead.

Checks (jobs/data_quality.py): freshness, volume vs trailing average, duplicate
rate on the idempotency key, NULL rate on critical columns, and referential
integrity of facts vs the CDC-maintained dimensions.  Results are appended to
`marts.data_quality_runs`, so "was it ever broken?" is a query, not a memory.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from haweye_common import make_dag, spark_bash

with make_dag(
    dag_id="data_quality_checks",
    schedule="*/15 * * * *",
    catchup=False,
    tags=["haweye", "quality", "streaming"],
    doc_md=__doc__,
    dagrun_timeout=timedelta(minutes=12),
) as dag:

    checks = spark_bash("iceberg_quality_checks", "data_quality", "--write-results",
                        execution_timeout=timedelta(minutes=8))

    # Consumer lag is the other half of "is the pipeline healthy": a job can write
    # perfectly and still be 6 hours behind.  Best-effort from inside compose.
    BashOperator(
        task_id="kafka_lag",
        bash_command=(
            "if docker ps >/dev/null 2>&1; then "
            "  docker exec haweye-kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 "
            "    --describe --all-groups 2>/dev/null | awk 'NR==1||$6>1000{print}' | head -20; "
            "else echo '(kafka CLI not reachable from the scheduler; use `make kafka-tail` on the host)'; fi"
        ),
    )

    report = BashOperator(
        task_id="report",
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command="echo 'quality run recorded in marts.data_quality_runs (see `make maintain-report`)'",
    )

    checks >> report
