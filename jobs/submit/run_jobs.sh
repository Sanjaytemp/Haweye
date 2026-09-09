#!/usr/bin/env bash
# Submit several streaming jobs at once (each in its own detached spark-submit),
# then show where to look:  docker compose logs -f spark-master
#
#   ./jobs/submit/run_jobs.sh                      # the three streaming jobs
#   ./jobs/submit/run_jobs.sh feature_store        # just one
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOBS=("$@")
if [[ ${#JOBS[@]} -eq 0 ]]; then
  JOBS=(streaming_ingestion feature_store real_time_scoring)
fi
for j in "${JOBS[@]}"; do
  echo ">>> submitting ${j}"
  "${HERE}/run_job.sh" --detach "${j}"
done
cat <<INFO

Streaming jobs are now running inside the spark-master container.
  logs      :  docker compose logs -f spark-master | grep '\\[feature_store\\]'
  status    :  ./jobs/submit/stop_jobs.sh --status
  stop      :  ./jobs/submit/stop_jobs.sh
  spark ui  :  http://localhost:8080  (and :4040 for the running application)
INFO
