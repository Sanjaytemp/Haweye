#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Submit a job from ./jobs.
#
#   ./jobs/submit/run_job.sh streaming_ingestion --starting earliest
#   ./jobs/submit/run_job.sh --detach train_model --days 7
#   ./jobs/submit/run_job.sh --local data_quality
#
# Modes
#   default : `docker compose exec spark-master spark-submit ...` (driver runs in
#             the cluster container; jars/catalog come from spark-defaults.conf)
#   --local : run `spark-submit --master local[N]` on your own machine with the
#             jars cached in ./.spark-jars (best for debugging in an IDE)
#   --detach: same as default but returns immediately, log -> ./.run/<job>.log
# -----------------------------------------------------------------------------
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
cd "${REPO_ROOT}"

[[ -f .env ]] && { set -a; . ./.env; set +a; }

COMPOSE="${COMPOSE:-docker compose}"
SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
MODE="${MODE:-cluster}"
EXTRA_SPARK_ARGS="${EXTRA_SPARK_ARGS:-}"

mode=""
job=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --local)        mode=local; shift ;;
    --detach)       mode=detach; shift ;;
    --cluster)      mode=cluster; shift ;;
    --)             shift; break ;;
    -*)             echo "wrapper: unknown flag $1 (use -- to pass flags to the job)" >&2; exit 2 ;;
    *)              if [[ -z "$job" ]]; then job="${1%.py}"; shift
                    else echo "wrapper: unexpected argument '$1'" >&2; exit 2; fi ;;
  esac
done
mode="${mode:-${MODE}}"

if [[ -z "$job" ]]; then
  echo "usage: $0 [--local|--detach] <job> [job args...]" >&2
  echo "available jobs:" >&2
  ls jobs/*.py | sed 's#jobs/##; s#\.py$##; s#^#  #' >&2
  exit 2
fi
if [[ ! -f "jobs/${job}.py" ]]; then
  echo "!!! jobs/${job}.py does not exist" >&2
  exit 1
fi

# ---------------------------------------------------------------- cluster mode
spark_submit_cluster() {
  local extra="$EXTRA_SPARK_ARGS"
  ${COMPOSE} exec ${COMPOSE_EXEC_ARGS:--T} spark-master bash -lc "
    set -euo pipefail
    export PYTHONPATH=/opt/spark/jobs:\${PYTHONPATH:-}
    spark-submit \
      --master '${SPARK_MASTER}' \
      --name '${job}' \
      ${extra} \
      /opt/spark/jobs/${job}.py $*
  "
}

# ------------------------------------------------------------------ local mode
ensure_local_jars() {
  local dir="${REPO_ROOT}/.spark-jars"
  if [[ -s "${dir}/.ready" ]]; then return 0; fi
  echo ">>> one-time jar download into ${dir}"
  mkdir -p "${dir}"
  local py="${LOCAL_PYTHON:-python3}"
  "${py}" scripts/download_jars.py "${dir}" \
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION:-1.5.2}" \
    "org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION:-3.3.4}" \
    "software.amazon.awssdk:bundle:${AWS_SDK_VERSION:-2.25.6}" \
    "org.postgresql:postgresql:${POSTGRES_JDBC_VERSION:-42.7.3}"
  echo "ok" > "${dir}/.ready"
}

spark_submit_local() {
  ensure_local_jars
  if ! command -v spark-submit >/dev/null 2>&1; then
    cat >&2 <<MSG
!!! spark-submit not found.
    Either run without --local (uses the Docker cluster), or install a local
    Spark:  python3 -m venv .venv && .venv/bin/pip install pyspark==${SPARK_VERSION:-3.5.1}
    Then re-run with:  PATH="$PWD/.venv/bin:\$PATH" $0 --local ${job}
MSG
    exit 3
  fi
  local jars
  jars="$(find "${REPO_ROOT}/.spark-jars" -name '*.jar' | paste -sd, -)"
  export PYTHONPATH="${REPO_ROOT}/jobs:${PYTHONPATH:-}"
  export SPARK_MASTER="local[${LOCAL_SPARK_THREADS:-2}]"
  export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
  export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
  export AWS_S3_ENDPOINT="${AWS_S3_ENDPOINT:-http://localhost:${MINIO_API_PORT:-9000}}"
  export POSTGRES_HOST="${POSTGRES_HOST_LOCAL:-localhost}"
  export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
  export KAFKA_SERVERS="${KAFKA_SERVERS_LOCAL:-localhost:${KAFKA_HOST_PORT:-9094}}"
  export REDIS_HOST="${REDIS_HOST_LOCAL:-localhost}"
  spark-submit \
    --master "local[${LOCAL_SPARK_THREADS:-2}]" \
    --name "${job}" \
    --jars "${jars}" \
    --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS:-4}" \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.ui.enabled=false \
    ${EXTRA_SPARK_ARGS} \
    "jobs/${job}.py" "$@"
}

# ------------------------------------------------------------------- dispatch
if [[ "$mode" == "local" ]]; then
  spark_submit_local "$@"
elif [[ "$mode" == "detach" ]]; then
  mkdir -p .run
  nohup env MODE=cluster COMPOSE="${COMPOSE}" SPARK_MASTER="${SPARK_MASTER}" \
        EXTRA_SPARK_ARGS="${EXTRA_SPARK_ARGS}" \
        "${HERE}/run_job.sh" "${job}" "$@" > ".run/${job}.log" 2>&1 < /dev/null &
  pid=$!
  echo "${pid}" > ".run/${job}.pid"
  echo ">>> ${job}: submitted (pid ${pid}); log -> .run/${job}.log"
else
  spark_submit_cluster "$@"
fi
