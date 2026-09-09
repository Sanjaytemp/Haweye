"""Point the serving layer at a different model version (deploy / rollback).

    python jobs/model_refresh.py --list                 # what is in MinIO?
    python jobs/model_refresh.py --current              # what is live right now?
    python jobs/model_refresh.py --to v2026...          # deploy a specific version
    python jobs/model_refresh.py --rollback             # previous version
    python jobs/model_refresh.py --to latest --copy-to ./artifacts/models

Rollback is a pointer write, not a redeploy: the streaming job picks it up on
its next model refresh tick, the REST API on its next `POST /admin/refresh`.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import cli, config, sparkutils  # noqa: E402
from common import model as model_mod

JOB = "model_refresh"


def list_versions(spark) -> list[dict]:
    jvm_path = model_mod._path(spark, config.MODEL_URI)
    fs = model_mod._hadoop(spark)
    out = []
    try:
        it = fs.listStatus(jvm_path)
    except Exception as exc:
        return [{"error": str(exc)}]
    for status in it:
        name = status.getPath().getName()
        if not name.startswith("v"):
            continue
        uri = f"{config.MODEL_URI.rstrip('/')}/{name}"
        out.append({"version": name, "uri": uri,
                    "bytes": status.getLen() if not status.isDirectory() else None,
                    "metadata": model_mod.load_metadata(spark, uri).get("metrics", {})})
    return sorted(out, key=lambda r: r["version"], reverse=True)


def main(argv=None) -> int:
    parser = cli.build_parser(JOB, extra=[
        (("--list",), {"action": "store_true"}),
        (("--current",), {"action": "store_true"}),
        (("--to",), {"default": None, "help": "version name, or 'latest'"}),
        (("--rollback",), {"action": "store_true"}),
        (("--copy-to",), {"default": None, "help": "also copy joblib+metadata to this host dir"}),
    ])
    args = parser.parse_args(argv)
    if cli.maybe_print_config(args):
        return 0
    cli.announce(JOB, args)

    spark = sparkutils.get_spark(JOB)
    if args.current or not (args.to or args.rollback or args.list):
        print(json.dumps(model_mod.describe(spark), indent=2, default=str), flush=True)
    if args.list:
        print(json.dumps(list_versions(spark), indent=2, default=str), flush=True)
        return 0

    versions = [v["version"] for v in list_versions(spark)]
    if not versions:
        print(">>> no model versions found; run the training job first", flush=True)
        return 1
    target = args.to
    if args.rollback:
        if len(versions) < 2:
            print(">>> nothing to roll back to", flush=True)
            return 1
        current = (model_mod.read_current_version(spark) or {}).get("version")
        idx = versions.index(current) if current in versions else 0
        target = versions[min(idx + 1, len(versions) - 1)]
    if target in (None, "latest"):
        target = versions[0]
    uri = f"{config.MODEL_URI.rstrip('/')}/{target}"
    if not model_mod.exists(spark, uri):
        print(f">>> {uri} does not exist", flush=True)
        return 1
    if args.dry_run:
        print(f">>> would point version.txt at {uri}", flush=True)
        return 0
    meta = model_mod.load_metadata(spark, uri)
    from common import features as feat_mod

    if meta.get("features") and meta["features"] != feat_mod.model_feature_names():
        print("!!! feature list differs from the code's current feature list — "
              "this model will score with `model_score IS NULL` until the jobs are redeployed",
              flush=True)
    model_mod.write_version_pointer(spark, target, uri + "/spark_model", str(meta.get("git_sha", "")))
    print(f">>> version.txt -> {target}", flush=True)
    if args.copy_to:
        model_mod.publish_serving_copy(spark, uri, args.copy_to)
        print(f">>> copied to {args.copy_to}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
