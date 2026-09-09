#!/usr/bin/env bash
# MLflow tracking server for this project (optional stack: `--profile mlflow`).
#
# Two stores, and knowing which is which explains 90% of MLflow confusion:
#   backend store  -> WHERE runs/params/metrics/tags live (here: sqlite on a volume)
#   artifact store -> WHERE the model files live          (here: the same MinIO bucket)
#
# We deliberately do NOT spin up a Postgres just for MLflow: it is a learning
# aid in this repo, not the system of record.  The system of record for "which
# model is live" is `models/fraud_rf/version.txt` in MinIO (see
# jobs/common/model.py), because the streaming scorer can read that without
# MLflow being up.  Swap BACKEND_STORE for postgresql://… when you promote it.
set -euo pipefail

BACKEND_STORE="${MLFLOW_BACKEND_STORE_URI:-sqlite:////mlflow/mlflow.db}"
ARTIFACT_ROOT="${MLFLOW_ARTIFACT_ROOT:-s3://${LAKEHOUSE_S3_BUCKET:-lakehouse}/mlflow}"
HOST_ADDR="0.0.0.0"
PORT=5000

mkdir -p "$(dirname "${BACKEND_STORE#sqlite:////}")" 2>/dev/null || true

echo ">>> mlflow: backend=${BACKEND_STORE} artifacts=${ARTIFACT_ROOT}"
echo ">>> UI: http://localhost:${MLFLOW_PORT:-5000}"

# `--wait-for-ready-in-ui-url`? no: keep the command minimal and copy-pasteable.
exec mlflow server \
  --backend-store-uri "${BACKEND_STORE}" \
  --default-artifact-root "${ARTIFACT_ROOT}" \
  --host "${HOST_ADDR}" \
  --port "${PORT}"
