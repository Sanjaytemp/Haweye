#!/usr/bin/env bash
# Register (or refresh) the Debezium connector with Kafka Connect, then watch it.
#
#   ./cdc/register_connector.sh            # create/update + tail the status
#   ./cdc/register_connector.sh --status
#   ./cdc/register_connector.sh --pause | --resume | --delete
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f .env ]] && { set -a; . ./.env; set +a; }

CONNECT_URL="${CONNECT_URL:-http://localhost:${CDC_CONNECT_PORT:-8083}}"
NAME="${CDC_CONNECTOR_NAME:-haweye-dimensions}"
COMPOSE="${COMPOSE:-docker compose}"
CDC_COMPOSE="${COMPOSE} -f docker-compose.cdc.yml"

post() {
  if ! curl -sf "${CONNECT_URL}/connector-plugins" >/dev/null 2>&1; then
    echo ">>> connect is not answering on ${CONNECT_URL} yet; waiting ..."
    for i in $(seq 1 40); do
      sleep 3
      curl -sf "${CONNECT_URL}/connector-plugins" >/dev/null 2>&1 && break
    done
  fi
  local exists
  exists=$(curl -s -o /dev/null -w '%{http_code}' "${CONNECT_URL}/connectors/${NAME}")
  if [[ "${exists}" == "200" ]]; then
    echo ">>> updating existing connector ${NAME}"
    curl -sf -X PUT "${CONNECT_URL}/connectors/${NAME}/config" \
      -H 'Content-Type: application/json' -d @cdc/connectors/${NAME}.json >/dev/null
  else
    echo ">>> creating connector ${NAME}"
    curl -sf -X POST "${CONNECT_URL}/connectors" -H 'Content-Type: application/json' \
      -d @cdc/connectors/${NAME}.json >/dev/null
  fi
}

status() {
  echo "--- connector ${NAME} ---"
  curl -s "${CONNECT_URL}/connectors/${NAME}/status" |
    python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception: print("  (no status yet)"); raise SystemExit(0)
print("  state:", d.get("connector",{}).get("state"), "|", (d.get("connector",{}).get("trace") or "")[:180])
for t in d.get("tasks", []):
    print("  task ", t.get("id"), t.get("state"), (t.get("trace") or "")[:180])
' || true
  echo "--- captured topics (message counts) ---"
  local topics
  topics=$(${CDC_COMPOSE} exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list 2>/dev/null \
            | grep -E "cdc|haweye" || true)
  if [[ -z "${topics}" ]]; then
    echo "  (none yet - is the connector RUNNING and did the snapshot finish?)"
  fi
  while IFS= read -r t; do
    if [[ -z "$t" ]]; then continue; fi
    # sum of end offsets = how many records Debezium has written to that topic
    n=$(${CDC_COMPOSE} exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
           --bootstrap-server localhost:29092 "$t" --time -1 2>/dev/null \
           | awk -F: '{s+=$3} END {print s+0}')
    printf '  %-34s %s records\n' "$t" "${n:-0}"
  done <<< "${topics}"
  dlq="${NAME}.dlq"
  d=$(${CDC_COMPOSE} exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
         --bootstrap-server localhost:29092 "$dlq" --time -1 2>/dev/null \
         | awk -F: '{s+=$3} END {print s+0}')
  # errors.tolerance=all means failures are silent UNLESS you look here
  printf '  %-34s %s (dead-letter; must be 0)\n' "$dlq" "${d:-0}"
  if [[ "${d:-0}" != "0" ]]; then
    echo "  >>> rejections in the DLQ; inspect with:"
    echo "      ${CDC_COMPOSE} exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \\"
    echo "        --bootstrap-server localhost:29092 --topic ${dlq} --from-beginning --max-messages 5"
  fi
}

case "${1:-}" in
  --status) status ;;
  --pause)  curl -sf -X PUT "${CONNECT_URL}/connectors/${NAME}/pause" ;;
  --resume) curl -sf -X PUT "${CONNECT_URL}/connectors/${NAME}/resume" ;;
  --delete) curl -sf -X DELETE "${CONNECT_URL}/connectors/${NAME}" && echo "deleted" ;;
  *) post; sleep 5; status ;;
esac
