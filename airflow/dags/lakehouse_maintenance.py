"""DAG — Iceberg housekeeping: the job nobody writes until the tables get slow.

Streaming commits every 10 seconds: 8,640 snapshots/day, hundreds of tiny files.
Without maintenance you get (a) slow queries, (b) unbounded S3 storage, (c) a
catalog table that grows forever.  The three Iceberg procedures fix them:

    rewrite_data_files   bin-pack small parquet files into ~128MB ones
    expire_snapshots     drop history older than N days (keep replay window)
    remove_orphan_files  delete parquet no snapshot references (failed commits)

Ordering matters: compact first (creates new files), expire second (makes the old
ones unreferenced), orphan-clean third (deletes them).  That is why they are
sequential tasks in one DAG rather than three schedules.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.operators.bash import BashOperator
from haweye_common import make_dag, spark_bash

with make_dag(
    dag_id="lakehouse_maintenance",
    schedule="0 3 * * *",                    # after training (02:30) so we do not fight for CPU
    catchup=False,
    tags=["haweye", "lakehouse", "maintenance"],
    doc_md=__doc__,
    dagrun_timeout=timedelta(hours=3),
) as dag:

    before = BashOperator(
        task_id="before_report",
        bash_command="./jobs/submit/run_job.sh table_maintenance --report-only",
    )
    compact = spark_bash("rewrite_data_files", "table_maintenance",
                         "--skip-expire", "--skip-orphans", timeout_s=3600)
    expire = spark_bash("expire_snapshots", "table_maintenance",
                        "--skip-compact", "--skip-orphans", "--older-than-days", "7")
    clean = spark_bash("remove_orphan_files", "table_maintenance",
                       "--skip-compact", "--skip-expire", "--orphan-days", "3")
    after = BashOperator(
        task_id="after_report",
        bash_command="./jobs/submit/run_job.sh table_maintenance --report-only",
    )
    vacuum = BashOperator(
        task_id="catalog_vacuum",
        bash_command=(
            "psql \"${AIRFLOW_CONN_LAKEHOUSE_PG:-postgresql://haweye:haweye@postgres:5432/catalog}\" -c "
            "'VACUUM ANALYZE iceberg_catalog.iceberg_tables' 2>/dev/null || "
            "echo 'vacuum skipped (psql not available or catalog empty)'"
        ),
    )

    before >> compact >> expire >> clean >> after >> vacuum
