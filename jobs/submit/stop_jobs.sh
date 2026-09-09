#!/usr/bin/env bash
# Ask every running streaming job to stop *gracefully* (finish the current
# micro-batch, commit it, then exit).  The jobs install a SIGTERM handler for
# exactly this (see jobs/common/sparkutils.py::install_graceful_stop).
#
#   ./jobs/submit/stop_jobs.sh              # stop the three streaming jobs
#   ./jobs/submit/stop_jobs.sh --status     # just look
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[[ -f .env ]] && { set -a; . ./.env; set +a; }
COMPOSE="${COMPOSE:-docker compose}"
ALL=(streaming_ingestion feature_store real_time_scoring cdc_merge_stream)
JOBS=()
STATUS=0
for a in "$@"; do
  case "$a" in
    --status) STATUS=1 ;;
    *) JOBS+=("$a") ;;
  esac
done
if [[ ${#JOBS[@]} -eq 0 ]]; then JOBS=("${ALL[@]}"); fi

for j in "${JOBS[@]}"; do
  pid="$( ${COMPOSE} exec -T spark-master bash -lc "ps -eo pid,args | grep -F -- '${j}.py' | grep -v grep | awk '{print \$1}' | head -1" || true )"
  if [[ -z "${pid}" ]]; then
    echo "  ${j}: not running"
    continue
  fi
  if [[ ${STATUS} -eq 1 ]]; then
    echo "  ${j}: running (pid ${pid})"
    continue
  fi
  echo "  ${j}: SIGTERM -> ${pid} (finishing current micro-batch)"
  ${COMPOSE} exec -T spark-master bash -lc "kill ${pid} || true" || true
done
if [[ -d .run ]]; then
  for f in .run/*.pid; do
    [[ -e "$f" ]] || continue
    p="$(cat "$f")"
    kill "${p}" 2>/dev/null || true
    rm -f "$f"
  done
fi
echo ">>> done"
