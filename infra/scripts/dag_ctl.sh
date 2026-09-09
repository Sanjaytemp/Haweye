#!/usr/bin/env bash
# Pause/unpause every haweye DAG.  Airflow pauses *newly discovered* DAG files by
# default, so after your first `make airflow-up` the schedules are "visible but
# sleeping" until this runs.  Inside the airflow container as the entrypoint helper.
set -uo pipefail
ACTION="${1:-unpause}"
DAGS=(fraud_model_training lakehouse_maintenance data_quality_checks cdc_dimension_merge)
for d in "${DAGS[@]}"; do
  case "$ACTION" in
    unpause) airflow dags unpause "$d" >/dev/null 2>&1 && echo "  running:   $d" || echo "  missing:   $d" ;;
    pause)   airflow dags pause   "$d" >/dev/null 2>&1 && echo "  paused:    $d" ;;
    status)  airflow dags list 2>/dev/null | grep -E 'dag_id|haweye|fraud|lakehouse|quality|cdc' ;;
    *) echo "usage: $0 [unpause|pause|status]" >&2; exit 2 ;;
  esac
done
