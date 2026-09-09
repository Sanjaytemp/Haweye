#!/usr/bin/env python3
"""Check a NiFi instance and (best effort) start the flow built in the UI.

Read this before trusting any "auto-deploy NiFi from a script" tool, including
this one: NiFi provisioning properly is done with **NiFi Registry + flow version
control** or a `nifi-toolkit`/`NiFi scripting` setup, because the REST API's
processor-creation calls are fragile (they need revisions, positions, and the
exact type names of whatever image version you run).

So this script does the two robust things:
  1. waits for NiFi and reports version/processor types available
  2. if the three processors already exist on the canvas (you built them once,
     or imported `nifi/flow`), it *starts/stops* them via REST.

    python3 scripts/deploy_nifi_flow.py --url http://localhost:8090/nifi
    python3 scripts/deploy_nifi_flow.py --start
    python3 scripts/deploy_nifi_flow.py --stop
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

WANTED = ("GenerateFlowFile", "ExecuteScript", "PublishKafka")


def req(url: str, method: str = "GET", body: dict | None = None, timeout: int = 15):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read()[:300].decode(errors="replace")}
    except Exception as exc:
        return 0, {"error": f"{type(exc).__name__}: {exc}"}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="haweye NiFi helper")
    ap.add_argument("--url", default=os.environ.get("NIFI_URL", "http://localhost:8090/nifi"))
    ap.add_argument("--wait", type=int, default=90)
    ap.add_argument("--start", action="store_true")
    ap.add_argument("--stop", action="store_true")
    ap.add_argument("--list", action="store_true", help="list processors + their state")
    args = ap.parse_args(argv)
    url = args.url.rstrip("/")

    deadline = time.time() + args.wait
    status: dict = {}
    while time.time() < deadline:
        code, body = req(f"{url}/flow/process-groups/root")
        if code == 200:
            status = body
            break
        time.sleep(3)
    if not status:
        print("!!! NiFi is not answering.  Start it with: docker compose --profile nifi up -d",
              file=sys.stderr)
        return 1

    procs = status.get("processors", []) or []
    print(f">>> NiFi canvas has {len(procs)} processor(s)")
    wanted = [p for p in procs if any(w in (p["component"].get("name", "") + p["component"].get("type", ""))
                                      for w in WANTED)]
    for p in procs:
        c = p["component"]
        print(f"    - {c.get('name', '?'):28s} {c.get('type', '').split('.')[-1]:24s} "
              f"state={c.get('state')}")
    if not wanted:
        print("\n>>> The flow is not on the canvas yet.  That is intentional: see "
              "nifi/README.md for the 6-step build (GenerateFlowFile -> ExecuteScript "
              "-> PublishKafka), or skip NiFi and use `make gen-stream`.")
        return 0
    if not (args.start or args.stop or args.list):
        return 0
    action, state = ("RUNNING", "start") if args.start else ("STOPPED", "stop")
    for p in wanted:
        c = p["component"]
        code, _ = req(f"{url}/flow/process-groups/root/{action}/{c['id']}", "PUT",
                      {"revision": p["revision"]})
        print(f"    {state} {c.get('name')}: HTTP {code}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
