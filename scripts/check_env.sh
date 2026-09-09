#!/usr/bin/env bash
# Is the platform actually up?  Read-only checks with copy-paste fixes.
# Run it any time: `make check`
set -uo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f .env ]] && { set -a; . ./.env; set +a; }
COMPOSE="${COMPOSE:-docker compose}"
FAIL=0
ok()   { printf '  \033[32mOK \033[0m %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m %s\n     -> %s\n' "$1" "$2"; FAIL=$((FAIL+1)); }
warn() { printf '  \033[33mWARN\033[0m %s\n     -> %s\n' "$1" "$2"; }

echo "== docker =="
if ! command -v docker >/dev/null 2>&1; then
  bad "docker not installed" "see docs/08-setup-macos-linux-windows.md"; exit 1
fi
if ! docker info >/dev/null 2>&1; then
  bad "docker daemon not running" "start Docker Desktop, or: sudo systemctl start docker"; exit 1
fi
ok "docker daemon reachable ($(docker --version | head -1))"

echo "== containers =="
for svc in kafka minio postgres redis spark-master; do
  state="$(${COMPOSE} ps --format '{{.Service}} {{.State}}' 2>/dev/null | awk -v s="$svc" '$1==s{print $2; exit}')"
  extra="$(${COMPOSE} ps --format '{{.Service}} {{.Status}}' 2>/dev/null | awk -v s="$svc" '$1==s{$1="";print; exit}')"
  if [[ "$state" == "running" ]]; then ok "${svc}: ${extra:-running}"
  else bad "${svc} not running" "docker compose up -d ${svc}"; fi
done
for svc in airflow-webserver serving-api; do
  state="$(${COMPOSE} ps --format '{{.Service}} {{.State}}' 2>/dev/null | awk -v s="$svc" '$1==s{print $2; exit}')"
  if [[ "$state" == "running" ]]; then ok "${svc}: running"
  else warn "${svc} not running" "optional: make up-full"; fi
done

echo "== object storage =="
if ${COMPOSE} exec -T minio sh -c "curl -sf -o /dev/null http://localhost:9000/minio/health/live" 2>/dev/null; then
  ok "minio health endpoint"
else
  bad "minio not answering" "docker compose logs minio"
fi

echo "== postgres =="
pg() { ${COMPOSE} exec -T postgres psql -U "${POSTGRES_USER:-haweye}" -tAc "$1" 2>/dev/null | tail -1; }
for db in catalog lakehouse airflow; do
  if [[ "$(pg "SELECT 1 FROM pg_database WHERE datname='${db}'")" == "1" ]]; then ok "database ${db}"
  else bad "database ${db} missing" "volumes only run init scripts when empty: make nuke && make up"; fi
done
n_tables="$(pg "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_catalog='lakehouse'")"
if [[ "${n_tables:-0}" -gt 3 ]]; then ok "lakehouse public tables: ${n_tables}"
else bad "dimension/serving tables missing" "make seed-dims"; fi
cat_rows="$(pg "SELECT count(*) FROM iceberg_catalog.table"`true` 2>/dev/null || true)"
if [[ "${cat_rows}" =~ ^[0-9]+$ ]]; then ok "iceberg catalog rows: ${cat_rows}"
else warn "iceberg catalog not initialised" "normal before the first job run"; fi

echo "== kafka =="
topics="$(${COMPOSE} exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | tr '\n' ' ')"
if [[ "${topics}" == *raw_transactions* ]]; then ok "topics: ${topics}"
else bad "raw_transactions missing" "docker compose up -d kafka-topics"; fi

echo "== spark =="
if ${COMPOSE} exec -T spark-master bash -lc "ls /opt/spark/jars | grep -qi iceberg" 2>/dev/null; then
  ok "iceberg jar in the spark image"
else
  bad "iceberg jar missing" "docker compose build spark-master"
fi
if ${COMPOSE} exec -T spark-master bash -lc "python3 -c 'import redis, psycopg2'" 2>/dev/null; then
  ok "executor python deps (redis, psycopg2)"
else
  bad "executor python deps missing" "docker compose build spark-master"
fi
if curl -sf -o /dev/null "http://localhost:${SPARK_UI_PORT:-8080}/" 2>/dev/null; then
  ok "spark master ui: http://localhost:${SPARK_UI_PORT:-8080}"
else
  warn "spark master ui" "not reachable yet (may still be starting)"
fi
if curl -sf -o /dev/null "http://localhost:${MINIO_CONSOLE_PORT:-9001}/" 2>/dev/null; then
  ok "minio console: http://localhost:${MINIO_CONSOLE_PORT:-9001}"
else
  warn "minio console" "not reachable yet"
fi

echo
if [[ ${FAIL} -eq 0 ]]; then
  printf '\033[32mEverything essential is up.\033[0m Next: make seed-dims && make data-backfill\n'
else
  printf '\033[31m%d problem(s).\033[0m Fix FAIL lines; WARN lines are optional services.\n' "${FAIL}"
fi
exit ${FAIL}
