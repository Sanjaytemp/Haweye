#!/usr/bin/env bash
# Airflow container entrypoint: wait for the metadata DB, initialise it exactly
# once (schema + admin user), then hand over to the real airflow command.
set -euo pipefail

ROLE="${1:-scheduler}"
shift || true

export AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"
INIT_FLAG="${AIRFLOW_HOME}/.initialized"

wait_for_postgres() {
  local host="${POSTGRES_HOST:-postgres}"
  local port="${POSTGRES_PORT:-5432}"
  local db="${AIRFLOW_DB_NAME:-airflow}"
  echo ">>> waiting for postgres ${host}:${port}/${db} ..."
  python3 - "$host" "$port" "$db" <<'PY'
import socket, sys, time
host, port, db = sys.argv[1], int(sys.argv[2]), sys.argv[3]
deadline = time.time() + 180
while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=3):
            print(">>> postgres is accepting connections")
            sys.exit(0)
    except OSError:
        time.sleep(2)
print("!!! postgres never became reachable", file=sys.stderr)
sys.exit(1)
PY
}

initialise() {
  wait_for_postgres
  mkdir -p "${AIRFLOW_HOME}/dags" "${AIRFLOW_HOME}/logs" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    if airflow db migrate 2>/tmp/airflow-db.err; then
      break
    fi
    echo ">>> db migrate failed (retrying)"; cat /tmp/airflow-db.err || true; sleep 5
  done
  airflow users create \
    --username "${AIRFLOW_ADMIN_USER:-admin}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname Admin --lastname User --role Admin \
    --email "admin@haweye.local" >/dev/null 2>&1 \
    || echo ">>> admin user already exists (ok)"
  touch "${INIT_FLAG}" 2>/dev/null || true
}

case "${ROLE}" in
  init)
    initialise
    airflow dags list 2>/dev/null || true
    echo ">>> airflow-init finished"
    ;;
  webserver|scheduler)
    initialise
    echo ">>> starting airflow ${ROLE}"
    exec airflow "${ROLE}" "$@"
    ;;
  *)
    exec airflow "${ROLE}" "$@"
    ;;
esac
