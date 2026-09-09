"""Iceberg table properties, centralised so every table gets the same safety
belt.  Each one solves a concrete problem we hit in the streaming path:

  write.format.default     parquet  -> columnar, splittable, cheap ML scans
  delete.distribution      none    -> small row counts; skip an extra shuffle
  write.distribution       hash(<part key>) -> fewer, larger files per partition
  write.target-file-size   128MB   -> streaming loves many tiny files; batch
                                      ML loves few big ones.  Compaction (the
                                      maintenance DAG) reconciles the two.
"""
from __future__ import annotations

RAW = {
    "write.format.default": "parquet",
    "write.distribution-mode": "hash",
    "write.target-file-size-bytes": "134217728",
}

FEATURES = {
    "write.format.default": "parquet",
    "write.distribution-mode": "hash",
    "write.target-file-size-bytes": "134217728",
    # feature tables get point lookups from the backfill / API fallback paths
    "read.parquet.vectorization.batch-size": "1024",
}

DIMENSION = {
    "write.format.default": "parquet",
    "write.distribution-mode": "none",
    "write.target-file-size-bytes": "67108864",
}

STATE = {
    "write.format.default": "parquet",
    "write.distribution-mode": "none",
    "write.target-file-size-bytes": "67108864",
}

LABELS = {
    "write.format.default": "parquet",
    "write.distribution-mode": "hash",
}

QUARANTINE = {
    "write.format.default": "parquet",
    "write.distribution-mode": "none",
}

#: partition expressions per table (Iceberg hidden partitioning - you never
#: manage directories, the transform is stored in metadata and pruning is
#: automatic when you filter on `dt` / `event_ts_ts`)
PARTITION_BY = {
    "raw.transactions_raw": ["dt"],
    "raw.transactions_enriched": ["dt"],
    "features.transactions_feature_v1": ["dt"],
    "raw.fraud_labels": ["dt"],
    "raw.card_state": ["dt"],
    "raw.load_failures": ["dt"],
}
