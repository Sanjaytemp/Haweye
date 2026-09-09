# 01 — Architecture: what talks to whom, and why

If you read only one doc before changing code, read this one: almost every bug in
a pipeline like this is a *boundary* bug, and this file is a map of the boundaries.

---

## 1. The two clocks

There are two independent loops, and confusing them causes most beginner pain:

| | the **streaming** loop | the **batch** loop |
|---|---|---|
| triggered by | a Kafka partition having new rows | the clock (Airflow cron) |
| latency | seconds | hours/days |
| writes | appends, `MERGE`s, Redis/Postgres | tables + model registry |
| failure mode | lag grows, features go stale | a run is missed, the model is old |
| code | `jobs/streaming_ingestion.py`, `feature_store.py`, `real_time_scoring.py`, `cdc_merge_stream.py` | `backfill_training_data.py`, `train_model.py`, `table_maintenance.py`, `data_quality.py` |

They meet in exactly two places, both by *table*, never by RPC:

1. the **feature table** (`features.transactions_feature_v1`): streaming writes
   it, training reads it;
2. the **model pointer** (`models/fraud_rf/version.txt`): training writes it,
   scoring reads it every micro-batch.

That's the whole coupling. No streaming job ever calls Airflow; Airflow never
restarts a stream.

---

## 2. The streaming path, hop by hop

```
generator (or NiFi, or a real acquirer)
   │  JSON, one line per transaction, key = card_id
   ▼
Kafka topic raw_transactions                      3 partitions, 24h retention
   │
   ▼  readStream (format=kafka, startingOffsets=latest, maxOffsetsPerTrigger)
JOB 1  jobs/streaming_ingestion.py
   │  from_json(TRANSACTION_SCHEMA)  ──► parse
   │  quality_checks()               ──► good rows | bad rows
   │  dedupe on md5(transaction_id)  ──► no double-count inside the batch
   │  MERGE INTO raw.transactions_enriched (append via NOT MATCHED)
   │  bad rows ─────────────────────► raw.load_failures   (quarantine, not restart)
   ▼
Iceberg  raw.transactions_enriched   partitioned by days(event_ts), upsert key transaction_id
   │
   ▼  readStream (format=iceberg, incremental)  + dim.merchants + dim.card_accounts + raw.card_day_agg
JOB 2  jobs/feature_store.py
   │  enrichment_sql()   joins dimensions, derives country_mismatch / *_hit / ratios
   │  rolling_features_sql()  5min/1h windows (event-time RANGE frames)
   │  micro_batch_aggs_sql() ─► MERGE INTO raw.card_minute_agg   (cheap 24h history)
   │  derived_features_sql()  ratios, z-score, calendar
   │  features ─► MERGE INTO features.transactions_feature_v1
   │           ─► Redis feat:txn:<id>, feat:card:<id>  (TTL 24h)
   │           ─► Kafka transactions_features          (optional, for other consumers)
   ▼
JOB 3  jobs/real_time_scoring.py
   │  Spark ML PipelineModel.transform ─► prediction
   │  rules.apply_rules ─► rule_score, rule_hits        (SQL mirror of api/serve.py)
   │  final = 0.75*model + 0.25*rules  (rules alone while no model is published)
   │  decision = decline|review|monitor|approve ; is_alert = final >= threshold
   │  ─► Postgres serving.fraud_scores (upsert), serving.fraud_alerts (insert)
   │  ─► Redis score:txn:<id>   ─► Kafka transactions_scores
   ▼
REST API (api/serve.py)  reads Redis → Postgres; serves the same decision + the
                          exact feature vector, and can score a *hypothetical* txn
```

**Why three jobs and not one?** Because each has a different failure and a
different scale. Ingestion must never stop (it's the only thing buffering
traffic). Features are the CPU-heavy part and you want to rescale them alone.
Scoring touches three stores and is where you'd add a second model. One mega-job
means one restart restarts everything and re-reads Kafka.

**Why is `raw.transactions_enriched` written before features exist?** So that a
feature bug can be fixed by *replaying from the table* (`backfill_training_data.py`)
rather than hoping Kafka still has the data. The stream is the pipe; the lake is
the truth.

---

## 3. The batch path

```
Airflow (02:30) ──► jobs/data_quality.py ──gate──► jobs/backfill_training_data.py (optional)
                                                   │  rebuild features + labels for 30 days
                                                   ▼
                                          jobs/train_model.py
                                             ├ time-based split, no random shuffle
                                             ├ RF/GBT/LR + VectorAssembler inside the pipeline
                                             ├ metrics.json + feature importance
                                             ├ if auc >= --min-auc-to-publish:
                                             │     write version.txt  (THE deploy)
                                             │     copy sklearn_model.joblib to ./artifacts/models
                                             └ MLflow run (optional, if URI set)
Airflow (03:00) ──► jobs/table_maintenance.py: rewrite_data_files → expire_snapshots
                                                    → remove_orphan_files → VACUUM catalog
```

Publishing is moving a **text file** (`version.txt`). The streaming scorer notices
on its next micro-batch. That is a zero-downtime deploy with no orchestrator
involved, and `--rollback` writes the previous value back.

---

## 4. The CDC path (optional, `make cdc-up`)

```
Postgres dimensions db (wal_level=logical, publication haweye_cdc)
   │  Debezium reads the WAL
   ▼
Kafka Connect → cdc.public.merchants / cdc.public.card_accounts
   │              {"op":"u","before":{...},"after":{...},"source":{"lsn":...,"ts_ms":...}}
   ▼
jobs/cdc_merge_stream.py (streaming)   or   jobs/cdc_merge_batch.py (Airflow, every 5 min)
   │  parse_cdc_stream  → latest_change_per_key (deletes win ties)
   │  MERGE INTO dim.merchants
   │      WHEN MATCHED AND new.source_ts_ms >= old.source_ts_ms THEN UPDATE   ← out-of-order safe
   │      WHEN MATCHED AND op='DELETE' AND new.source_ts_ms >= old THEN DELETE
   │      WHEN NOT MATCHED AND op<>'DELETE' THEN INSERT
   └─ also mirrors into Postgres serving.*_lakehouse so the API can show "what the
      model saw" next to the business record
   ▼
dim.merchants, dim.card_accounts  ← joined by JOB 2 for enrichment
```

Two design notes worth keeping in your head:

* the merge is **gated on `source_ts_ms`**, so replaying the topic from the
  beginning cannot corrupt the table — this is what makes the whole thing
  re-runnable;
* the dimension tables live in Iceberg as *mirrors*. Business writes go to
  Postgres. Never write dimensions directly into Iceberg; the next CDC event
  will overwrite you.

---

## 5. Where everything physically lives

| what | where | who writes it | who reads it |
|---|---|---|---|
| events (24h buffer) | Kafka volumes | generator/NiFi | jobs 1–3 |
| `lakehouse` bucket `warehouse/raw/…` | MinIO | job 1 | job 2, backfill, quality |
| `warehouse/features/…` | MinIO | job 2 | training, job 3, API |
| `warehouse/dim/…` | MinIO | CDC merge | job 2 |
| `warehouse/marts/…` | MinIO | scoring, DQ, maintenance | analysts, runbook |
| `warehouse/models/fraud_rf/…` | MinIO | train_model | job 3 |
| `./artifacts/models/` (bind mount) | host | train_model / model_refresh | REST API |
| catalog (which snapshot is current) | Postgres db `catalog`, schema `iceberg_catalog` | Iceberg itself | every Spark session |
| `serving.*` | Postgres db `lakehouse` | jobs 2–3, API | API, humans |
| `feat:*`,`score:*` | Redis (TTL 24h) | jobs 2–3 | API (falls back to Postgres) |
| Airflow state | Postgres db `airflow` | Airflow | Airflow |

If you can name the file a value lives in, you can debug it. Every "table is
empty" question in `06-runbook.md` is really a row of this table.

---

## 6. Guarantees we actually provide

| property | how | where to verify |
|---|---|---|
| no duplicate writes from Kafka replays | `dedupe` inside the batch + `MERGE … WHEN NOT MATCHED` across batches | `tests/integration/test_pyspark_sql.py::test_dedupe_is_idempotent…` |
| restart-safe streaming | checkpoint in object storage, `startingOffsets=latest` is only for the first start | `jobs/common/sparkutils.py:start_query` |
| poisoned record doesn't stop the stream | `_corrupt_record` + quality flags → `raw.load_failures` | `jobs/common/schema.py:split_good_and_bad` |
| dimension updates are order-safe | ts-gated `MERGE` | `jobs/common/cdc.py:merge_changes` |
| no label leakage | labels never enter the wire payload | `tests/unit/test_generator_contract.py` |
| model/rule parity (stream vs API) | shared `rules` definitions + randomised parity test | `tests/unit/test_api_contract.py::test_api_rules_and_spark_rules_agree_on_random_rows` |
| a bad model can't ship | AUC floor blocks the pointer move; guardrail task fails the DAG | `jobs/train_model.py`, `airflow/dags/fraud_model_training.py` |
| schema drift fails loudly | typed `from_json` + explicit `FEATURES_TABLE_SCHEMA` | `jobs/common/{schema,features}.py` |

What we deliberately **don't** provide (and what production adds): real
end-to-end exactly-once (Iceberg has no 2PC sink here — we rely on idempotent
MERGE), key management (creds are in `.env`), authentication/authorisation on the
API beyond an optional shared token, multi-AZ durability, and a feature *registry*
with CI-approved renames.

Next: **[02-first-run.md](02-first-run.md)**.
