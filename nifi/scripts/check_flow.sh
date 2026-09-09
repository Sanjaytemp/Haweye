#!/usr/bin/env bash
# Ask a running NiFi what it is doing - the read-only half of `nifi/README.md`.
# Mounted into the container at /opt/nifi/nifi-current/scripts, and usable from
# the host too (point NIFI_URL at the published port).
#
#   docker compose --profile nifi exec nifi bash scripts/check_flow.sh
#   NIFI_URL=http://localhost:8090/nifi ./nifi/scripts/check_flow.sh
set -uo pipefail
URL="${NIFI_URL:-http://localhost:8090/nifi}"

say() { printf '%-22s %s\n' "$1" "$2"; }

command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 2; }

code=$(curl -s -o /dev/null -w '%{http_code}' "${URL}/flow/process-groups/root" || echo 000)
say "canvas endpoint" "HTTP ${code}"
if [ "$code" != "200" ]; then
  echo ">>> NiFi is not answering at ${URL}"
  echo ">>> start it with:  docker compose --profile nifi up -d nifi"
  exit 1
fi

echo
echo ">>> processors on the canvas (name | state | type):"
curl -s "${URL}/flow/process-groups/root" > /tmp/haweye-nifi-canvas.json
if command -v python3 >/dev/null 2>&1; then
  python3 - <<'PY'
import json

with open('/tmp/haweye-nifi-canvas.json') as fh:
    canvas = json.load(fh)

procs = canvas.get('processors') or []
if not procs:
    print("  (empty canvas - build the 3-processor graph: see nifi/README.md)")
for p in procs:
    c = p['component']
    name = (c.get('name') or c.get('id'))[:34]
    kind = (c.get('type') or '').split('.')[-1]
    print(f"  {name:34s} {c.get('state', '?'):10s} {kind}")

print()
print(f">>> {len(procs)} processor(s). Expected 3: GenerateFlowFile, ExecuteScript, PublishKafka.")
PY
else
  say "processors" "(no python3 in this image; read the UI instead)"
fi

echo
echo ">>> need real traffic without clicking?  make gen-stream"
