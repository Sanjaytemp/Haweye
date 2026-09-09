# Haweye — real-time transaction monitoring, feature store and fraud ML

A complete, runnable implementation of the "Real-Time Transaction Monitoring &
Feature Store with ML" project: card transactions stream in, get enriched and
scored in seconds, land in an Iceberg lakehouse, and a model trained nightly
decides — with a REST API answering *why* for any single transaction.

It is built to be taken apart: every piece works alone, every job is one file you
can read top to bottom, and `docs/` explains each technology as if you have never
seen it (see [docs/00-beginner-tour.md](docs/00-beginner-tour.md)).

```
 generator ──► Kafka ──► Spark Structured Streaming ──► Iceberg on MinIO ──► Redis + Postgres ──► FastAPI
   (source)    (buffer)      parse·enrich·features         (the truth)          (online store)     (answers)
                                   ▲                              ▲
                        Airflow ───┘ nightly training + lakehouse maintenance
                        Debezium ───┘ CDC: merchants & card_accounts stay fresh (optional)
```

| concern | technology | in this repo |
|---|---|---|
| event source | python generator (NiFi optional) | `generator/`, `nifi/` |
| buffer | Kafka 7.6.1 (KRaft, no ZooKeeper) | `docker-compose.yml` |
| compute | Spark 3.5.1 — Structured Streaming + batch, same code | `jobs/` |
| table format | Iceberg 1.5.2 on MinIO, JdbcCatalog in Postgres | `jobs/common/sparkutils.py`, `docs/03-iceberg.md` |
| dimensions in, not batched | Debezium 2.6 + Kafka Connect, `MERGE INTO` Iceberg | `cdc/`, `jobs/common/cdc.py` |
| online store | Redis (features) + Postgres (scores, alerts) | `api/serve.py`, `jobs/common/io.py` |
| orchestration | Airflow 2.8 — training, maintenance, quality, CDC | `airflow/dags/` |
| ML | pyspark.ml pipeline (RF/GBT/LR) + sklearn sidecar, MLflow optional | `jobs/train_model.py`, `docs/07-ml.md` |

---

## Quick start

Requirements: Docker (12 GB RAM to it), `make`, git. Details per OS:
[docs/08-setup-macos-linux-windows.md](docs/08-setup-macos-linux-windows.md).

```bash
git clone https://github.com/Sanjaytemp/Haweye.git
cd Haweye
make bootstrap      # checks, .env, .venv, validates config, builds images
make up             # kafka, minio, postgres, redis, spark  (all healthy)
make check          # scripts/check_env.sh: what's up, what's misconfigured
make seed-dims      # dimension tables CDC will replicate
make data-backfill-small        # 3 days of labelled history -> Iceberg
make jobs-up && make gen-stream # the three streaming jobs + live traffic
make train-model    # nightly batch: time-split training, publishes to MinIO
make api-up && make api-smoke   # one decision, with the exact features used
```

Expected last line of `make api-smoke`:

```json
{"transaction_id":"HYP-…","score":0.94,"decision":"decline",
 "rule_hits":["EXTREME_TICKET","CNP_ABROAD","AMOUNT_SPIKE_1H"],
 "model_version":"v20260909T090512Z","latency_ms":3.1}
```

Then: `make sql` (spark-sql against the lakehouse), `make maintain-report` (file
counts/snapshots), `make airflow-up` → http://localhost:8085 (admin/admin), and the
CDC demo below. Full narrated version with what to look at after each command:
[docs/02-first-run.md](docs/02-first-run.md).

Stop with `make down` (keeps everything) or `make nuke` (empty project again).

## CDC: the same dimensions, replicated not reloaded

```bash
make cdc-up      # 2nd Postgres (wal_level=logical) + its own Kafka + Connect + connector
make cdc-demo    # dimension INSERTs/UPDATEs/DELETEs, printing the SQL it runs
make cdc-sync    # one merge: Kafka CDC topics -> Iceberg MERGE  (or: make cdc-stream)
make cdc-status  # connector + task state, records per topic, DLQ depth
make cdc-check   # row-count parity: Postgres vs the lakehouse
```

Deletes delete, out-of-order events can't clobber newer rows (the merge is gated
on `source_ts_ms`), and replaying the topic from the beginning is harmless.
Explanation, the Postgres prerequisites, and how to operate it without filling the
disk: [docs/05-cdc.md](docs/05-cdc.md).

## What is in the box

```
docker-compose.yml         the platform (15 services, profiles for the optional ones)
docker-compose.cdc.yml     the CDC stack (separate Kafka + Connect, so a nuke can't strand a WAL slot)
Makefile                   every operation, one target each — `make help`
jobs/
  streaming_ingestion.py   Kafka -> parse -> quality gates -> quarantine -> Iceberg
  feature_store.py         rolling features -> Iceberg + Redis + Kafka   (--verify: stream==batch)
  real_time_scoring.py     model + rules -> decision -> Postgres alerts, Redis, Kafka
  train_model.py           time-split training, metrics, publish pointer
  backfill_training_data.py  history for training/feature tables (rebuild derived state)
  cdc_merge_stream.py / cdc_merge_batch.py   Debezium events -> MERGE INTO dim.*
  table_maintenance.py     rewrite_data_files, expire_snapshots, remove_orphan_files
  data_quality.py          freshness / duplicates / nulls / referential integrity
  model_refresh.py         show, promote or roll back the live model version
  common/                  schema·enrichment·features·rules·cdc·io·model·sparkutils·config
  submit/run_job.sh        one entry point: cluster (default), --local, --detach
api/serve.py               FastAPI: score, features, card, merchant, alerts, stats, model
generator/                 the synthetic acquirer (deterministic, seeded, labelled)
cdc/                       setup SQL, connector payload, register/status script, change simulator
sql/                       roles+databases, dimensions, serving tables, catalog grants (initdb-ordered)
airflow/dags/              training (with AUC guardrail), maintenance, quality, CDC merge
infra/                     Dockerfiles + spark/airflow/kafka configs, all versions pinned
nifi/                      optional ingestion graph (3 processors) + how to build it
tests/                     83 unit tests (no JVM, ~2s) + pyspark integration tests
docs/                      00 tour · 01 architecture · 02 first run · 03 iceberg · 04 streaming
                           05 cdc · 06 runbook · 07 ml · 08 setup · 09 git/github
scripts/                   bootstrap · check_env · validate_compose · download_jars ·
                           check_cdc_parity · apply_sql · make_pr
.github/workflows/         CI (lint+unit, pyspark with a JVM, docs links) + nightly compose E2E
```

### Compose profiles

Everything optional is behind a profile, so `make up` stays light:

| profile | adds | when |
|---|---|---|
| *(none)* | kafka, minio, postgres, redis, spark | always — the core pipeline |
| `generator` | the transaction generator as a service | when you'd rather `docker compose up` than `make gen-stream` |
| `api` | `serving-api` (FastAPI on :8000) | you want the REST layer |
| `airflow` | airflow-init, webserver, scheduler (:8085) | schedules, backfills, retries |
| `mlflow` | MLflow tracking server (:5000) | comparing runs in a UI |
| `nifi` | NiFi (:8090) | you want the visual ingestion graph from the brief |
| `merge` | `spark-cdc` (in `docker-compose.cdc.yml`) | running the CDC merge inside the CDC stack |
| `full` | all of the above | demo day / `make up-full` |

Example: `docker compose --profile api --profile airflow up -d`.

## Data you can query

| table | what | written by |
|---|---|---|
| `lake.raw.transactions_raw` | every event, dedup-keyed, replay-safe | job 1 (MERGE) |
| `lake.raw.transactions_enriched` | + dimensions joined, quality flags | job 1 (append) |
| `lake.features.transactions_feature_v1` | the feature vectors (offline store) | job 2 (MERGE) |
| `lake.raw.card_minute_agg` / `card_day_agg` | pre-aggregated history that makes 24h windows cheap | job 2 |
| `lake.raw.fraud_labels` | ground truth, joined at training time only | backfill / dispute feed |
| `lake.raw.load_failures` | quarantined rows with reasons | job 1 |
| `lake.dim.merchants` / `card_accounts` | dimensions, CDC-maintained | CDC merge |
| `lake.marts.data_quality_runs` | every check result, forever | `data_quality.py` |
| Postgres `serving.*` | scores, alerts, feature + dimension mirrors, `v_open_alert_detail` | jobs 2–3, API |
| Redis `feat:*`, `score:*` | the same vectors, TTL 24h, for ms reads | jobs 2–3 |

Time travel works because these are Iceberg tables:
`SELECT * FROM lake.features.transactions_feature_v1 FOR VERSION AS OF <snapshot_id>`
(and `… .snapshots` to find the id).

## Quality gates (what CI runs)

```bash
make lint      # ruff + compose/env/volume validation + every generated SQL parsed + bash -n
make test      # 83 unit tests: rules, feature definitions, generator contract, API, repo checks
make test-spark# same SQL executed on a JVM (CI does this; needs java)
make test-all  # lint + unit
```

Two of these are worth calling out because they are the tests that catch the bugs
that matter here:

* `test_wire_payload_never_leaks_ground_truth` — the generator must not put
  `label`/velocity on the wire, or the AUC is fiction;
* `test_api_rules_and_spark_rules_agree_on_random_rows` — 3,000 randomised rows
  through both rule engines (Spark SQL and the API's python), since the two
  implementations are deliberately separate (the API container has no JVM).

## Design decisions, with reasons

* **Rules and a model, blended** (`0.75·model + 0.25·rules`). A cold pipeline has
  no model: while none is published the rules carry the decision at full weight,
  so the system can always *decline* (see `jobs/common/rules.blend`). Rules are
  also the explanation an analyst can repeat to a customer.
* **`--once` on every streaming job.** Same file as a 24/7 daemon and as a batch
  run, so backfills, Airflow scheduling and CI all reuse the production code path.
* **24h features from aggregate tables**, not 24-hour stream windows: correctness
  per row at 1/8640th the state.
* **Idempotent writes everywhere** (keyed `MERGE`), because Kafka is at-least-once
  and a restart must never need a cleanup job.
* **Labels in their own table, time-based splits, AUC floor before publishing.**
  Three boring rules that keep the ML honest.
* **CDC for dimensions, Kafka for facts.** Dimensions change rarely but must be
  fresh at decision time; facts are born as events.
* **Iceberg's streaming source reads appends only** — so job 1 appends to the
  enriched table that job 2 streams, and the dedupe MERGE goes to the raw table.
  A one-line reorder there silently stops feature updates
  ([docs/04-streaming.md](docs/04-streaming.md#reading-iceberg-as-a-stream)).

## Troubleshooting

| symptom | look here first |
|---|---|
| a container restarts / `health: starting` forever | `make ps`, `docker compose logs <svc>`; on Mac/Windows: Docker's RAM limit |
| "features are all NULL" | `make seed-dims`, then `lake.dim.*` counts (join miss) — `docs/06-runbook.md` |
| nothing in Iceberg though Kafka has data | `.run/*.log`, then `lake.raw.load_failures` (quarantined? why?), then `make jobs-status` |
| lag grows and never shrinks | partitions vs consumers, `STREAM_TRIGGER_SECONDS`, executor memory |
| `Failed to find data source: iceberg` | the runtime jars: `.spark-jars/.ready` (local mode) or rebuild the image |
| job dies with `Table or view not found: lake.…` | the catalog/db wasn't created yet: run `make up` then `make seed-dims`; `spark-defaults.conf` must be mounted |
| API: `"model": {"loaded": false}` | expected before the first `make train-model`; else `ls artifacts/models/` |
| CDC connector FAILED, "relation … does not exist" | publication/table names, `cdc/sql/00_cdc_setup.sql`, `REPLICA IDENTITY FULL` |
| Postgres disk full (CDC stack) | an inactive replication slot retains WAL — `docs/05-cdc.md#operating-safely` |
| port already allocated | change the `*_PORT` in `.env` (never `docker compose down -v` to "fix" a port) |

The long version, symptom by symptom, with the command that distinguishes the
possible causes: **[docs/06-runbook.md](docs/06-runbook.md)**.

## Never used git or GitHub?

[docs/09-github-for-beginners.md](docs/09-github-for-beginners.md) starts at
"`cd` means change directory" and ends with you opening a merged pull request.
Short version:

```bash
git switch -c my-change
# edit, then
./scripts/make_pr.sh        # runs the gates, commits, pushes, opens the PR
```

## Extending it (good first tasks)

1. A batch-id ledger table so the Postgres/Redis writes are exactly-once too.
2. `--target-alerts-per-day`: pick the threshold hitting a review capacity and
   store it in the model metadata.
3. A Grafana dashboard over `serving.fraud_scores` + `marts.data_quality_runs`.
4. A `tests/e2e/` job that asserts a planted fraud transaction ends up declined.
5. Kafka Avro/Schema Registry instead of JSON — `common/schema.py` is the seam.

`CONTRIBUTING.md` covers the dev loop; `CHANGELOG.md` follows Keep-a-Changelog.

## Known limitations (deliberate, and where the trade-off is written down)

* Single-broker Kafka, one worker, `replication-factor 1` — no HA ([docs/01](docs/01-architecture.md)).
* `sasl_scram` placeholders only; real deployments need TLS + ACLs + secret management.
* The REST API's auth is one optional shared token (`API_TOKEN`), and the model is
  read from a bind mount rather than a model server.
* Features are computed per micro-batch from bounded history, not from unbounded
  keyed state (`flatMapGroupsWithState` would be faster, and much harder to read).
* No schema registry: JSON with a typed contract, enforced at the boundary
  (`from_json` + quarantine).

MIT licensed, see [LICENSE](LICENSE).
