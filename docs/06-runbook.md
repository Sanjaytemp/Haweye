# 06 — Runbook: symptoms → commands

*Ordered by "how likely is this at 11pm". Each entry is: what you see, the three
things it is usually caused by, the command that tells you which one, and the fix.
Everything is copy-pasteable from the repo root.*

## First, always: the 60-second triage

```bash
make ps        # is anything restarting/exited that shouldn't be?
make check     # does each service answer and look configured right?
make jobs-status
tail -40 .run/streaming_ingestion.log .run/feature_store.log .run/real_time_scoring.log
docker compose ps --format '{{.Name}} {{.Status}}'   # "Up 3 (health: starting)" = still booting
```

Two habits that save hours:

* **Read the last log line, not the first.** The exception at the top of a stack
  trace is usually the consequence.
* **Ask "which table is empty?"** Nine of ten problems in this architecture are
  one hop not writing. `make sql` then:

```sql
SELECT 'raw', count(*) FROM lake.raw.transactions_enriched
UNION ALL SELECT 'features', count(*) FROM lake.features.transactions_feature_v1
UNION ALL SELECT 'labels', count(*) FROM lake.raw.fraud_labels
UNION ALL SELECT 'quarantine', count(*) FROM lake.raw.load_failures
UNION ALL SELECT 'dims', count(*) FROM lake.dim.merchants;
```

The row where the count stops growing is the job to look at.

---

## "Nothing is arriving in Kafka"

```bash
make kafka-tail                      # prints from raw_transactions
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

| cause | tell | fix |
|---|---|---|
| generator not running | `make ps` shows no `run` container | `make gen-stream` (it's `docker compose run`, so it is not in `ps` when you Ctrl-C it) |
| wrong bootstrap server | log says `BrokerTransportFailure` / `Local: Broker pipe failed` | inside a container use `kafka:29092`; from your laptop `localhost:9094`. Never `localhost:9092` from a container. |
| topic missing | `--list` lacks `raw_transactions` | `docker compose up -d kafka-topics` (one-shot creator) |
| payload never sent (producer error swallowed) | generator log has `Message delivery failed` | `docker compose logs generator` |

## "Kafka has data but Iceberg doesn't"

```bash
./jobs/submit/run_job.sh streaming_ingestion --once --starting earliest
tail -50 .run/streaming_ingestion.log
```

1. **Job isn't running.** `.run/*.pid` is a hint, not truth: `stop_jobs.sh
   --status` execs into the container and greps `ps` for the real pids.
2. **All rows quarantined.** Check `lake.raw.load_failures`:
   `SELECT failure_reasons, count(*) FROM lake.raw.load_failures GROUP BY 1 ORDER BY 2 DESC`
   → `["corrupt_json"]` = the producer changed format; `["bad_country"]` = a
   new country not in `schema.COUNTRIES` (fix both sides; there is a test for this).
3. **Checkpoint says "already done".** `--once --starting earliest` re-reads; if
   the table stays empty *and* the log says `rows=0`, the topic is empty
   (retention deleted it after 24h — this is a demo data lifetime, not a bug).

## "Features are NULL / all zeros"

The join found nothing. In order of probability:

```bash
docker compose exec -T postgres psql -U haweye -d lakehouse -c "select count(*) from public.merchants"
# 0  → make seed-dims
./jobs/submit/run_job.sh cdc_merge_batch --dry-run     # what would the merge write
spark-sql> select count(*), max(source_ts_ms) from lake.dim.merchants;
# stale max(source_ts_ms) → CDC not running: make cdc-status
```

Then look at what the join produced:

```sql
SELECT count(*) total,
       count(merchant_risk_score) has_risk,
       count(credit_limit) has_limit,
       count(DISTINCT merchant_id) merchants_seen
FROM lake.features.transactions_feature_v1 WHERE dt = current_date();
```

`has_risk = 0` with rows present = dimension keys don't match (case, padding, or
`char(2)` truncation — see `docs/05-cdc.md`). `merchants_seen` far below 120 = the
generator is using merchant ids the seed doesn't have (seed with the same
`GEN_SEED`).

## "Lag keeps growing"

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups | sort -k6 -nr | head
```

| symptom | fix |
|---|---|
| LAG grows, batch duration ~trigger interval | raise `STREAM_TRIGGER_SECONDS` (20–30s) or add partitions; 3 partitions = 3 readers max |
| LAG grows, batch duration ≫ interval | your heavy op is the 1h window: check `spark.sql.shuffle.partitions` (4 on a laptop), give the executor more memory in `.env` |
| one partition lagging, others at 0 | key skew: one card dominating (that's `--card CARD-00042` in the demo). Real systems: salting or more partitions |
| LAG constant but nonzero | normal: it is the backlog between producer and 10s trigger |
| `maxOffsetsPerTrigger` reached every batch | you capped it; that cap *is* the back-pressure, raise it deliberately |

## "The Airflow DAG failed"

```bash
docker compose logs --tail=100 airflow-scheduler
docker compose exec airflow-scheduler airflow tasks list fraud_model_training
docker compose exec airflow-scheduler airflow dags list-runs -d fraud_model_training
```

* `check_feature_readiness` failed → the quality gate did its job. Read
  `marts.data_quality_runs`; fix the data, then re-trigger
  (`make airflow-train-now`). Do **not** delete the gate.
* `train_model` failed with `not enough rows after the time split` → your window
  has fewer than `--days 30` of history: `make data-backfill` first, or lower the
  DAG's `--days`.
* `guardrail_auc` failed → metrics were written and AUC < 0.70. The pointer was
  **not** moved (previous model still serving). Check label rate: `select
  count(*) filter (where label=1) / count(*) from lake.raw.fraud_labels`.
* Everything fails with `run_job.sh: command not found` → the repo isn't mounted
  where the image expects: `HAWEYE_REPO` must be `/opt/airflow` (set in compose).
* New DAG not listed → it's paused: `make airflow-unpause`; check
  `airflow dags report` for import errors.

## "The model version didn't change"

```bash
./jobs/submit/run_job.sh model_refresh --list
docker compose exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker compose exec -T minio mc ls -r local/lakehouse/models/fraud_rf
```

The pointer only moves when `auc >= --min-auc-to-publish` **and** `--publish
true`. `>>> pointer NOT moved (...)` in the training log is the answer. To deploy
an older version on purpose: `make model-rollback`.

After publishing, the *running* scoring job re-reads `version.txt` on its next
micro-batch — but if you set `MODEL_ARTIFACTS_DIR`, the API needs
`curl -X POST localhost:8000/v1/model/refresh` (or its own mtime watcher).

## "The API says model: not loaded"

Expected before the first training run: scoring still works (rules only). If it
says it after training:

```bash
ls -l artifacts/models/            # sklearn_model.joblib + metadata.json + version.txt
docker compose exec serving-api ls -l /opt/models
```

Missing on the host = the training container didn't have `MODEL_ARTIFACTS_DIR`
mounted (`./artifacts/models:/opt/models` on `spark-master`); missing in the
container = `serving-api` started before the directory existed → `make api-up`
again.

## "Spark can't find the Iceberg jars / ClassNotFoundException"

```bash
ls .spark-jars/.ready 2>/dev/null && echo "cached" || ./scripts/download_jars.py
```

The image bakes them at build time; `--local` mode downloads to `.spark-jars/`.
If `repo1.maven.org` is unreachable where you are, set `MAVEN_BASE_URL` to a
mirror in `.env` and `make build`. The exact symptom of missing jars is
`Failed to find data source: iceberg` or `IcebergSparkSessionExtensions` not
found — always the jars, never your code.

## "Postgres disk is growing" / CDC stalled

See `docs/05-cdc.md#operating-safely`. Short version: an inactive replication slot
retains WAL; `make cdc-status` shows the connector state, and
`SELECT * FROM pg_replication_slots` shows `active=false` + a large
`restart_lsn` gap.

## "Everything is slow"

```bash
docker stats --no-stream                 # who is eating CPU
./jobs/submit/run_job.sh table_maintenance --report-only
```

`total-data-files` in the thousands per partition → run `make maintain` now, then
fix the cause (trigger interval too small, or maintenance DAG never unpaused).
Feature scans slow with few files → you're querying without a `dt` filter; the
tables are partitioned by `dt` and Iceberg prunes on it.

## "I want to start over" (safe, in escalating order)

```bash
make jobs-down && make jobs-up        # restart the streams
docker compose restart spark-master   # cluster-side state
make down                             # stop everything, keep volumes
make nuke && make up && make seed-dims && make data-backfill-small   # empty lake
```

`make nuke` deletes Postgres too, so the seed steps after it are not optional —
that's the state where "features are all NULL" appears if you skip them.

## Health probes for CI / uptime checks

| check | command | meaning |
|---|---|---|
| stack | `./scripts/check_env.sh; echo $?` | exit code = number of failures |
| freshness | `./jobs/submit/run_job.sh data_quality --only-table features` | non-zero when the newest row is too old |
| parity | `python scripts/check_cdc_parity.py` | source rows == lakehouse rows |
| end-to-end | `make api-smoke` | one decision out of the whole pipeline |

## Logging knobs

`SPARK_LOG_LEVEL=WARN` (job noise), `--verbose` on any job (INFO logging + extra
per-step prints), and **`--print-config`**, which exits without touching Spark and
shows exactly what the job resolved:

```bash
./jobs/submit/run_job.sh feature_store --print-config
```

Worth running once, because `.env`, the container's `environment:` block and
`env_file` can all define the same variable — and the last one wins. If a value
looks wrong there, it is not your SQL.
