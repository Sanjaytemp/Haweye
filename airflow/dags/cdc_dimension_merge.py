"""DAG — CDC dimension merge, *scheduled* instead of always-on streaming.

This is the batch twin of `jobs/cdc_merge_stream.py`.  Use it when:

  * you do not want a 24/7 Spark job for two small dimension tables; or
  * you want the merge to be retriable, alertable and backfillable (a task queue,
    not a daemon).

Freshness becomes "every 5 minutes" instead of "every 5 seconds" — plenty for
merchant risk scores and card limits, and it costs nothing between runs.  The
Iceberg job reads *only the new CDC events* since its last commit (incremental
read), so an idle 5 minutes costs ~0.

The `check_connector` task is why this DAG survives a restart: if Debezium died
because Postgres was restarted mid-snapshot, you want that message, not an empty
merge that "succeeded".
"""
from __future__ import annotations

from datetime import timedelta

from airflow.operators.bash import BashOperator
from haweye_common import make_dag, spark_bash

with make_dag(
    dag_id="cdc_dimension_merge",
    schedule="*/5 * * * *",
    catchup=False,
    tags=["haweye", "cdc", "debezium"],
    doc_md=__doc__,
    dagrun_timeout=timedelta(minutes=10),
) as dag:

    connector_alive = BashOperator(
        task_id="check_connector",
        bash_command=(
            'curl -sf "http://localhost:${CDC_CONNECT_PORT:-8083}/connectors/'
            '${CDC_CONNECTOR_NAME:-haweye-dimensions}/status" '
            '| python3 -c "import json,sys; d=json.load(sys.stdin); '
            "print('connector', d['connector']['state'], 'tasks', "
            "[t['state'] for t in d['tasks']]); "
            "sys.exit(0 if d['connector']['state'] == 'RUNNING' else 1)\" "
            '|| { echo "CDC connect REST endpoint not reachable (is `make cdc-up` running?)"; '
            'test "${CDC_ENABLED:-false}" = "true"; }'
        ),
    )

    merge = spark_bash("merge_into_iceberg", "cdc_merge_batch", "--mirror-postgres",
                       execution_timeout=timedelta(minutes=8))

    verify = BashOperator(
        task_id="verify_parity",
        bash_command="python3 scripts/check_cdc_parity.py --quiet",
    )

    connector_alive >> merge >> verify
