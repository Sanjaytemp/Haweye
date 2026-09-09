# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Dates are the release dates of tagged versions, not of commits — an entry under
*Unreleased* is on a branch, not on `main`.

## [Unreleased]

Nothing yet — add your change here in the same PR that implements it.

## [0.1.0] - 2026-09-09

First working end-to-end implementation of the project brief.

### Added — platform

- `docker-compose.yml`: Kafka 7.6.1 (KRaft, dual listeners for container/host),
  MinIO + bucket initialiser, Postgres 16.3 (`wal_level=logical`, initdb SQL from
  `sql/`), Redis 7, Spark 3.5.1 master + worker from a custom image with the
  Iceberg 1.5.2 runtime and S3A jars, plus profile-gated `generator`, `api`,
  `airflow`, `mlflow` and `nifi` services.
- `docker-compose.cdc.yml`: a separate CDC stack — its own Postgres and Kafka (so a
  `make nuke` of the demo data cannot strand a WAL replication slot), Debezium 2.6.1
  Kafka Connect, a seeder, a dimension-change simulator, and an optional
  always-on merge worker (`profiles: merge`).
- Pinned versions and host ports in `.env.example`; `Makefile` exposing every
  operation (`make help`); `infra/config/spark/spark-defaults.conf` with the
  Iceberg `lake` catalog (JdbcCatalog in Postgres) and an s3a fallback catalog.
- `scripts/bootstrap`, `scripts/check_env.sh`, `scripts/download_jars.py`,
  `scripts/validate_compose.py` (YAML, mounts, interpolation, sqlglot parse of
  every generated SQL string, `jobs/common` import smoke test, JSON lint).

### Added — streaming and batch jobs

- `jobs/streaming_ingestion.py`: Kafka → typed `from_json` → nine quality gates →
  in-batch dedupe → replay-safe `MERGE` into `raw.transactions_raw` + append to
  `raw.transactions_enriched`; rejected rows go to `raw.load_failures` with reasons.
- `jobs/common/schema.py`: the transaction contract (single source of truth for the
  generator, the jobs and the tests), quarantine schema, `is_valid_payload`.
- `jobs/common/enrichment.py` + `jobs/common/dimensions.py`: dimension joins
  (merchant, card, category history) from one column spec that generates both the
  Postgres and the Iceberg DDL.
- `jobs/feature_store.py` + `jobs/common/features.py`: exact event-time `RANGE`
  windows (5 min / 1 h / online / international), cheap 24 h features from
  `raw.card_minute_agg` / `card_day_agg`, derived ratios and z-scores, one JSON
  projection shared by the offline table, Redis and Kafka, and `--verify` which
  re-derives every feature with the batch window function and reports the mean
  absolute difference.
- `jobs/real_time_scoring.py` + `jobs/common/rules.py`: eight weighted rules with
  both a SQL and a python implementation, `final = 0.75·model + 0.25·rules`
  (rules alone while no model is published), decisions, alert rows, Redis writes.
- `jobs/train_model.py`: time-based holdout, RF/GBT/LR, `StringIndexer` +
  `OneHotEncoder` + `VectorAssembler` inside the saved pipeline, AUC/precision/
  recall/F1 and threshold metrics, `metadata.json` (feature list, order, types,
  levels), an sklearn joblib sidecar for the JVM-free API, `--min-auc-to-publish`
  guardrail, optional MLflow run.
- `jobs/model_refresh.py`: show / promote / roll back the live version pointer.
- `jobs/backfill_training_data.py`: history for the feature and label tables,
  `--rebuild-state-only` for the aggregate tables.
- `jobs/table_maintenance.py`: `rewrite_data_files` → `expire_snapshots` →
  `remove_orphan_files` (+ optional `analyze`), `--report-only` for sizes/snapshots.
- `jobs/data_quality.py`: freshness, volume-vs-trailing-average, duplicate rate on
  the idempotency key, null rate on critical columns, referential integrity against
  the CDC-maintained dimensions; `--write-results` into `marts.data_quality_runs`.
- `jobs/submit/run_job.sh` (`cluster` default, `--local` with auto jar download,
  `--detach` with pid/log files), `run_jobs.sh`, `stop_jobs.sh` (`--status`).

### Added — CDC

- `cdc/sql/00_cdc_setup.sql`: publications, `REPLICA IDENTITY FULL`, a dedicated
  `REPLICATION` role, with the "why each line exists" notes.
- `jobs/common/cdc.py`: Debezium envelope parsing (with and without inline
  schema), latest-change-per-key with deletes winning ties, target-column discovery,
  ts-gated `MERGE INTO` (update / delete / insert), snapshot-position state table
  for incremental reads, and the connector payload generator shared with
  `cdc/connectors/haweye-dimensions.json`.
- `jobs/cdc_merge_stream.py` (seconds-fresh) and `jobs/cdc_merge_batch.py`
  (scheduled, Airflow-friendly) over one shared `apply_changes`; both mirror the
  dimensions into Postgres `serving.*_lakehouse` so the API can show what the
  scorer joined.
- `cdc/register_connector.sh`: create/update/pause/resume/status/delete, with
  per-topic record counts and the dead-letter depth in `--status`.
- `cdc/simulator.py`: dimension mutations (rename, risk-score change, limit change,
  close, reopen, insert, delete, burst) that print the SQL they run.
- `scripts/check_cdc_parity.py`: source-vs-lakehouse row parity, with fix hints.

### Added — serving layer

- `api/serve.py`: FastAPI on stdlib + psycopg2 + redis only (no JVM, no AWS SDK).
  `/healthz` (per-dependency status), `POST /v1/score` (score a hypothetical
  transaction from the online store + the published model), `GET /v1/score/{id}`,
  `GET /v1/features/{id}` (the exact vector the model saw), `GET /v1/card/{id}`,
  `GET /v1/merchant/{id}` (business record beside the lakehouse mirror),
  `GET/PATCH /v1/alerts`, `/v1/stats`, `/v1/model`, `POST /v1/model/refresh`.
  Redis-first with Postgres fallback; hot model reload off `version.txt` mtime;
  optional `X-API-Token`; score persistence is best-effort so a dependency outage
  degrades instead of failing the request.
- `api/Dockerfile`, `api/requirements.txt`.

### Added — orchestration

- `airflow/dags/fraud_model_training.py`: quality gate → train → AUC guardrail →
  show published version → refresh the serving layer → summary; `catchup=False`.
- `airflow/dags/lakehouse_maintenance.py`, `data_quality_checks.py` (every 15 min),
  `cdc_dimension_merge.py` (every 5 min, the batch alternative to the CDC stream).
- `airflow/dags/haweye_common.py`: one `run_job.sh` wrapper for every task, so a
  failed task can be reproduced by copy-pasting one line into a terminal.
- Airflow 2.8 image with the providers we use, connections-as-code, `admin` user
  bootstrap, and `infra/scripts/dag_ctl.sh` for unpause.

### Added — source simulator

- `generator/simulator.py`: merchants and cards with persistent behaviour
  (home merchants, category history, recent activity), injected fraud patterns
  (card testing, abroad CNP, ATM burst, category switch, amount spike, wire drain),
  a logistic ground-truth score, and deliberate label noise so no model can learn
  the generator.
- `generator/generator.py`: Kafka producer (confluent-kafka) and `--print`-only
  mode; `generator/profiles.py`: reads the same dimension ids from Postgres so
  streamed ids join successfully.
- `generator/seed_dimensions.py`: creates and fills the dimension tables from the
  repo DDL (idempotent).

### Added — optional NiFi path

- `nifi/README.md` (the 3-processor graph, in the UI, with the two gotchas),
  `nifi/flow/GenerateTransactions.groovy`, `nifi/scripts/check_flow.sh`,
  `scripts/deploy_nifi_flow.py`.

### Added — tests and CI

- 83 unit tests (`make test`, no Docker, no JVM): rule behaviour and the SQL/python
  parity of the two engines, generator contract + no-label-leakage, feature
  definitions and window semantics, every generated SQL string parsed with
  sqlglot, Debezium connector config, repo/compose/Makefile/docs consistency, and a
  TestClient-based API suite with stubbed Redis/Postgres.
- `tests/integration/test_pyspark_sql.py`: parsing, quarantine, dedupe
  idempotence, real window SQL against a pandas oracle, rule SQL vs python, the
  blend expression, Debezium envelope flattening, feature-store round-trip —
  executed on a JVM in CI (`actions/setup-java`), skipped locally when java is absent.
- `.github/workflows/ci.yml` (quality, spark, docs) and
  `.github/workflows/nightly-stack.yml` (compose E2E, including CDC parity).
- `pyproject.toml` with ruff/pytest/coverage configuration, `.gitignore`,
  `.gitattributes` (LF for scripts so containers never see `bash\r`),
  `.dockerignore`, `LICENSE` (MIT), this changelog, `CONTRIBUTING.md`.

### Added — documentation

- `docs/00-beginner-tour.md` (git, Docker, Kafka, Spark, Iceberg, MinIO, Redis,
  Airflow, CDC, NiFi, ML — one section each, plus a reading order for the code),
  `01-architecture.md` (the two clocks, every hop, where data physically lives,
  which guarantees we do and don't provide), `02-first-run.md` (step by step with
  expected output and "if not" branches), `03-iceberg.md`, `04-streaming.md`,
  `05-cdc.md` (including `#operating-safely`), `06-runbook.md`, `07-ml.md`,
  `08-setup-macos-linux-windows.md`, `09-github-for-beginners.md`.
- `README.md` rewritten: quick start, profile table, data map, quality gates,
  design decisions with reasons, troubleshooting table, known limitations.

### Fixed (found by the tests written for 0.1.0)

- `sparkutils`: `from pyspark.sql.streaming import Trigger` does not exist in
  Spark 3.5 and `DataStreamWriter.trigger()` is keyword-only — triggers are now
  built as kwargs, which is also what `--once` needs.
- `sparkutils.iceberg_source()`: replaced Paimon-style option names with the real
  Iceberg 1.5 streaming options (`stream-from-snapshot-id`,
  `stream-from-timestamp`, `streaming-skip-overwrite-snapshots`,
  `streaming-skip-delete-snapshots`) and raised on an unknown `--starting` value
  instead of silently ignoring it.
- Rules/API cold start: blending rules at 0.25 weight made a decline
  arithmetically impossible before the first model was published; when
  `model_score is None` the rules now carry the decision at full weight
  (`rules.blend`, `blend_sql`, `api/serve.py`), with a test pinning the
  equivalence of the SQL and python forms.
- Feature-name drift: the filtered rolling windows emitted `txn_count_online_1h`
  while the model contract, the minute-history pivot and the serving projection all
  used `online_txn_count_1h` — a missing column would have been silently filled
  with 0. Alias generation now derives every name from one function
  (`features.feature_alias`) and a test asserts the two lists cannot diverge.
- Generator/contract drift: the simulator could emit merchant countries
  (`PK`, `VN`, `BD`, `RU`, `UA`) that `schema.COUNTRIES` then quarantined. One
  allow-list, plus `test_generator_countries_are_a_subset_of_the_contract`.
- `api/serve.py` index route called `asdict()` on a dict (TypeError on `GET /`).
- Debezium key converter: `StringConverter` fails on the first captured row for a
  relational table (its key is a Struct) — both converters are JSON now, and
  `decimal.handling.mode=double` added.
- Compose: the `nifi` service was missing its `profiles:` key, so `make up` started
  it; the Postgres initdb mount pointed at a directory that did not exist; the
  generator service lacked a `working_dir`, so its `command` could not find
  `generator.py`; `x-airflow-common` was defined after `services:` and its anchor
  therefore resolved to nothing.
- `jobs/common/io.py`: the serving DDL was read from a non-existent
  `serving_tables.sql` (now `sql/20_serving.sql`), and `SQL_DIR`/`DDL_FILE`
  relative paths resolved outside the repo when run from the repo root.
- `sql/00_roles_and_databases.sql` created a `dimensions` database nothing used;
  the CDC source of truth is the `lakehouse` database, and the CDC stack has its
  own `dimensions` instance (see `docker-compose.cdc.yml`).
- Airflow DAGs referenced job flags that do not exist (`--days` on
  `backfill_training_data`, `--older-than-days` on `table_maintenance`,
  `--report-only` on `data_quality`), and the metrics file was written to a path
  the scheduler could not read.
- `train_model.py` now supports `--metrics-out`, so the nightly guardrail reads a
  file instead of scraping stdout.

### Notes for anyone upgrading from a hand-assembled starting point

- Iceberg table properties (compression, distribution mode, target file size) are
  centralised in `jobs/common/table_props.py`; docs/03 lists what is set and what is
  intentionally left at Iceberg's defaults.
- `MODEL_ARTIFACTS_DIR` (Spark side) and `MODEL_DIR` (API side) must point at the
  same mounted directory for hot model reload to work.
- The CDC connector's credentials are the `debezium` replication role, *not*
  `CDC_PG_USER` (which is the admin login used by the seed and the parity check).

[Unreleased]: https://github.com/Sanjaytemp/Haweye/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Sanjaytemp/Haweye/releases/tag/v0.1.0
