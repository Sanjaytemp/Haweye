"""Model registry on object storage — a "current version" pointer + immutable
per-version artifacts.  Deliberately boring and dependency-free; the optional
MLflow step in docs/07-ml.md replaces exactly this file with a real registry.

Layout on MinIO:

    s3a://lakehouse/models/fraud_rf/
        version.txt                  <- the pointer (one file, overwritten atomically-ish)
        v00000001/
            metadata.json            <- features, threshold, metrics, git sha
            spark_model/             <- PySpark PipelineModel (loaded by the scoring job)
            sklearn_model.joblib     <- loaded by the FastAPI serving layer
            feature_importance.csv   <- human-readable "why did it fire"
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config

POINTER_FILE = config.MODEL_POINTER_FILE


def _hadoop(spark: SparkSession):
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())


def pointer_path() -> str:
    return f"{config.MODEL_URI.rstrip('/')}/{POINTER_FILE}"


def read_current_version(spark: SparkSession) -> dict | None:
    """Read `version.txt` (csv: version,uri,trained_at,git_sha).  None if untrained."""
    path = pointer_path()
    try:
        df = spark.read.text(path)
    except Exception:
        return None
    rows = [r[0] for r in df.collect() if r and r[0].strip()]
    if not rows:
        return None
    parts = [p.strip() for p in rows[-1].split(",")]
    keys = ["version", "uri", "trained_at", "git_sha"]
    # strict=False on purpose: an old pointer line may carry only `version,uri`,
    # and the missing fields are optional metadata, not an error.
    return dict(zip(keys, parts[: len(keys)], strict=False))


def write_version_pointer(spark: SparkSession, version: str, uri: str, git_sha: str = "") -> None:
    cols = ["version", "uri", "trained_at", "git_sha"]
    trained_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    df = spark.createDataFrame([(version, uri, trained_at, git_sha)], cols)
    df.coalesce(1).write.mode("overwrite").option("header", "false").csv(pointer_path())


def timestamp_version() -> str:
    """Human-sortable, UTC, no clock races: v20260909T081500Z."""
    return "v" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def version_dir(spark: SparkSession, version: str) -> str:
    return f"{config.MODEL_URI.rstrip('/')}/{version}"


def next_version(spark: SparkSession) -> str:
    try:
        files = _hadoop(spark).listLocatedStatus(
            _path(spark, config.MODEL_URI)
        )
        seen = []
        while files.hasNext():
            name = files.next().getPath().getName()
            if name.startswith("v") and name[1:].isdigit():
                seen.append(int(name[1:]))
        current = max(seen) if seen else 0
    except Exception:
        current = 0
    return f"v{current + 1:08d}"


def _path(spark, uri: str):
    jvm = spark._jvm
    return jvm.org.apache.hadoop.fs.Path(uri)


def exists(spark: SparkSession, uri: str) -> bool:
    try:
        return _hadoop(spark).exists(_path(spark, uri + "/_SUCCESS")) or _hadoop(spark).exists(_path(spark, uri))
    except Exception:
        return False


def save_metadata(spark: SparkSession, version_dir: str, metadata: dict[str, Any]) -> None:
    """metadata.json is written *into* the version dir so consumers can self-check
    that the features they are about to score with match the ones trained on."""
    jvm_path = _path(spark, version_dir.rstrip("/") + "/metadata.json")
    fs = _hadoop(spark)
    payload = json.dumps(metadata, indent=2, default=str).encode()
    with fs.create(jvm_path, True) as out:
        out.write(payload)


def load_metadata(spark: SparkSession, version_dir: str) -> dict:
    try:
        txt = spark.read.text(version_dir.rstrip("/") + "/metadata.json").rdd.map(lambda r: r[0]).collect()
    except Exception:
        return {}
    try:
        return json.loads("".join(txt))
    except Exception:
        return {}


def load_pipeline_model(spark: SparkSession):
    """Return `(pipeline_model, version_info)`; `(None, info)` when untrained.

    The scoring job keeps working without a model — it just logs
    `model_score IS NULL` and the deterministic rules take over.  A pipeline
    must never be "down" because the ML part is missing.
    """
    info = read_current_version(spark) or {}
    if not info:
        return None, {"status": "no_pointer"}
    try:
        from pyspark.ml import PipelineModel

        model = PipelineModel.load(info["uri"])
        return model, {**info, "status": "loaded"}
    except Exception as exc:  # pragma: no cover
        return None, {**info, "status": f"load_failed:{exc}"}


def write_sklearn_bundle_from_file(spark: SparkSession, version_dir: str, local_path: str) -> str:
    """Upload an already-dumped joblib bundle to object storage."""
    fs = _hadoop(spark)
    dst = _path(spark, version_dir.rstrip("/") + "/sklearn_model.joblib")
    with open(local_path, "rb") as fh, fs.create(dst, True) as out:
        out.write(fh.read())
    return version_dir.rstrip("/") + "/sklearn_model.joblib"


def write_sklearn_bundle(spark: SparkSession, version_dir: str, model, metadata: dict) -> str:
    """Persist a sklearn joblib artifact next to the Spark model so the REST API
    can serve the *same* model without a JVM.  (Same training run, two formats.)
    """
    import os
    import tempfile

    import joblib

    local_dir = tempfile.mkdtemp(prefix="haweye_model_")
    local = os.path.join(local_dir, "sklearn_model.joblib")
    joblib.dump({"model": model, "metadata": metadata}, local)
    fs = _hadoop(spark)
    dst = _path(spark, version_dir.rstrip("/") + "/sklearn_model.joblib")
    with open(local, "rb") as fh, fs.create(dst, True) as out:
        out.write(fh.read())
    return version_dir.rstrip("/") + "/sklearn_model.joblib"


def write_local_pointer(out_dir: str, version: str, *, extra: str = "") -> str:
    """`version.txt` inside the shared model folder: what the API watches (mtime)."""
    path = os.path.join(out_dir, POINTER_FILE)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        fh.write(f"{version}{',' + extra if extra else ''}\n")
    os.replace(tmp, path)          # atomic swap: the API never reads half a file
    return path


def publish_serving_copy(spark: SparkSession, version_dir: str, out_dir: str,
                         version: str | None = None) -> None:
    """Copy the joblib + metadata into a host-mounted folder the API container reads.

    This keeps the serving API free of any AWS SDK: the model is simply a file on
    a shared bind mount (`./artifacts/models`).  In production you would instead
    give the API its own short-lived presigned URL / model-server.
    """
    fs = _hadoop(spark)
    os.makedirs(out_dir, exist_ok=True)
    for name in ("sklearn_model.joblib", "metadata.json"):
        src = version_dir.rstrip("/") + "/" + name
        if not fs.exists(_path(spark, src)):
            continue
        with fs.open(_path(spark, src)) as fh:
            data = bytes(fh.read())
        with open(os.path.join(out_dir, name), "wb") as out:
            out.write(data)
    if version:
        write_local_pointer(out_dir, version, extra=version_dir.rstrip("/"))


def score_with_model(spark: SparkSession, features: DataFrame):
    """Score a feature DataFrame with the current registered model.

    Returns (df, status).  On any model problem the original frame is returned
    untouched with `model_score = NULL` so that rules+alerting still run.
    """
    model, status = load_pipeline_model(spark)
    if model is None:
        return features.withColumn("model_score", F.lit(None).cast("double")), status
    try:
        scored = model.transform(features)
        return scored, status
    except Exception as exc:  # schema drift etc.
        print(f"!!! model.transform failed: {exc}", flush=True)
        return (features.withColumn("model_score", F.lit(None).cast("double")),
                {**status, "status": f"transform_failed:{type(exc).__name__}"})


def read_sklearn_bundle(spark: SparkSession, version_uri: str) -> dict:
    """Load the sklearn artifact + metadata from object storage (used by tests/CLI)."""
    meta = load_metadata(spark, version_uri)
    import tempfile

    hadoop = _hadoop(spark)
    local = os.path.join(tempfile.gettempdir(), "haweye_sklearn.joblib")
    with hadoop.open(_path(spark, version_uri.rstrip("/") + "/sklearn_model.joblib")) as src, \
            open(local, "wb") as dst:
        dst.write(src.read())
    import joblib

    return {"model": joblib.load(local), "metadata": meta}


def describe(spark: SparkSession) -> dict:
    info = read_current_version(spark) or {"version": "none"}
    out = {"model_uri": config.MODEL_URI, **info}
    if info.get("uri"):
        out["metadata"] = load_metadata(spark, info["uri"])
    return out
