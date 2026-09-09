# =============================================================================
# One entry point for every operation in this project.  `make help` lists them.
# Every target is a plain `docker compose` / `spark-submit` / `pytest` command
# you can copy into a terminal - nothing is hidden.
# =============================================================================
SHELL := /bin/bash
COMPOSE ?= docker compose
COMPOSE_CDC := $(COMPOSE) -f docker-compose.cdc.yml
# Use the venv `make bootstrap` created if it exists: ruff/pytest/sqlglot live
# there, and "No module named ruff" after a successful bootstrap is the single most
# confusing first-run error this repo could hand you. Override with `make lint PY=python3`.
VENV_PY = $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PY ?= $(VENV_PY)
GEN_IMG ?= python:$(or $(PYTHON_VERSION),3.11)-slim-bookworm

.DEFAULT_GOAL := help

# ------------------------------------------------------------------------------
# 0. setup & lifecycle
# ------------------------------------------------------------------------------
help:                  ## Show this help
	@echo "\033[1mHaweye — Real-Time Transaction Monitoring & Feature Store\033[0m"
	@grep -hE '^[a-zA-Z0-9_.-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo "\nStart here: docs/00-beginner-tour.md   then   make bootstrap"

bootstrap:             ## Prereq checks, .env, venv, image build (one time)
	./scripts/bootstrap

build:                 ## Build the custom spark + airflow + api images
	$(COMPOSE) build spark-master
	$(COMPOSE) --profile airflow build airflow-init
	$(COMPOSE) --profile api build serving-api

up:                    ## Core platform: kafka, minio, postgres, redis, spark
	$(COMPOSE) up -d
	$(COMPOSE) up -d --wait postgres minio
	./scripts/check_env.sh || true

up-full:               ## Everything: core + airflow + api + nifi-less generator
	$(COMPOSE) --profile airflow --profile api --profile generator up -d
	$(COMPOSE) up -d --wait postgres minio
	./scripts/check_env.sh || true

down:                  ## Stop containers (volumes and tables survive)
	$(COMPOSE) down
	-$(COMPOSE_CDC) down

nuke:                  ## Stop AND delete all data (fresh project state)
	$(COMPOSE) down -v --remove-orphans
	-$(COMPOSE_CDC) down -v --remove-orphans
	@echo ">>> volumes deleted; run 'make up' again"

ps:                    ## Container status
	@$(COMPOSE) ps -a; $(COMPOSE_CDC) ps -a 2>/dev/null || true

logs:                  ## Tail core logs (Ctrl-C to stop)
	$(COMPOSE) logs -f --tail=120

check:                 ## Verify every service is reachable and configured
	./scripts/check_env.sh

# ------------------------------------------------------------------------------
# 1. data: dimensions, labelled history, live stream
# ------------------------------------------------------------------------------
seed-dims:             ## Create + fill dimension tables in Postgres (CDC source of truth)
	$(COMPOSE) run --rm --no-deps -v $(CURDIR)/generator:/work -v $(CURDIR)/sql:/sql -w /work \
	  -e PG_HOST=postgres -e PG_DB=lakehouse -e SQL_DIR=/sql \
	  -e PG_USER=$(or $(POSTGRES_USER),haweye) -e PG_PASSWORD=$(or $(POSTGRES_PASSWORD),haweye) \
	  --entrypoint bash $(GEN_IMG) \
	  -c "pip install -q psycopg2-binary && python seed_dimensions.py"

data-backfill:         ## 30 days of labelled history -> Iceberg (features + labels)
	./jobs/submit/run_job.sh backfill_training_data --days 30 --rows-per-day 20000

data-backfill-small:   ## Same, but 3 days / 1500 rows per day (fast, for laptops)
	./jobs/submit/run_job.sh backfill_training_data --days 3 --rows-per-day 1500

gen-stream:            ## Stream live transactions into Kafka (stop with Ctrl-C)
	$(COMPOSE) run --rm -v $(CURDIR)/generator:/work -w /work \
	  -e KAFKA_SERVERS=kafka:29092 \
	  --entrypoint bash $(GEN_IMG) \
	  -c "pip install -q confluent-kafka && python generator.py --rate-per-sec $(or $(RATE),25)"

gen-burst:             ## A burst of obvious fraud, to watch rules + alerts fire
	$(COMPOSE) run --rm -v $(CURDIR)/generator:/work -w /work -e KAFKA_SERVERS=kafka:29092 \
	  --entrypoint bash $(GEN_IMG) \
	  -c "pip install -q confluent-kafka && python generator.py --rate-per-sec 12 --fraud-rate 0.9 \
	      --count 200 --card CARD-00042 --speed-up 60"

kafka-tail:            ## Print the raw topic (is the feed even arriving?)
	$(COMPOSE) exec -T kafka kafka-console-consumer --bootstrap-server localhost:9092 \
	  --topic raw_transactions --from-beginning --max-messages 10 --property print.key=true

# ------------------------------------------------------------------------------
# 2. streaming jobs (submitted into the Spark cluster)
# ------------------------------------------------------------------------------
jobs-up:               ## Start all three streaming jobs in the background
	./jobs/submit/run_jobs.sh

jobs-down:             ## Gracefully stop the streaming jobs (finish the micro-batch)
	./jobs/submit/stop_jobs.sh

jobs-status:           ## Which streaming jobs are alive?
	./jobs/submit/stop_jobs.sh --status

job-ingest:            ## Run job 1 in the foreground (see every log line)
	./jobs/submit/run_job.sh streaming_ingestion --starting earliest

job-features:          ## Run job 2 in the foreground
	./jobs/submit/run_job.sh feature_store --starting earliest

job-score:             ## Run job 3 in the foreground
	./jobs/submit/run_job.sh real_time_scoring --starting earliest

# ------------------------------------------------------------------------------
# 3. model
# ------------------------------------------------------------------------------
train-model:           ## Train on the last 30 days and publish to MinIO
	./jobs/submit/run_job.sh train_model --days 30 --holdout-days 5

model-current:         ## Which model is live?
	./jobs/submit/run_job.sh model_refresh --current

model-versions:        ## What is in the model registry (MinIO)?
	./jobs/submit/run_job.sh model_refresh --list

model-rollback:        ## Point the scorer at the previous version
	./jobs/submit/run_job.sh model_refresh --rollback

# ------------------------------------------------------------------------------
# 4. CDC (Debezium) — optional
# ------------------------------------------------------------------------------
cdc-up:                ## Start the CDC stack + register the Debezium connector
	$(COMPOSE_CDC) up -d postgres-cdc kafka connect cdc-seed
	$(COMPOSE_CDC) up -d --no-deps cdc-simulator
	./cdc/register_connector.sh

cdc-stream:            ## Run the CDC merge as a streaming job on the main cluster
	./jobs/submit/run_job.sh --detach cdc_merge_stream

cdc-sync:              ## One-shot CDC merge (what Airflow schedules every 5 min)
	./jobs/submit/run_job.sh cdc_merge_batch --mirror-postgres

cdc-demo:              ## Make dimension changes so you can watch them flow
	$(COMPOSE_CDC) exec -T cdc-simulator python3 /app/simulate_changes.py --rounds 5 --interval 3

cdc-status:            ## Connector + task state, and the captured topics
	./cdc/register_connector.sh --status

cdc-check:             ## Source row counts vs the Iceberg mirror (parity proof)
	$(COMPOSE) exec -T postgres psql -U $(or $(POSTGRES_USER),haweye) -d $(or $(CDC_PG_DB),dimensions) -c "SELECT count(*) AS merchants FROM public.merchants"
	$(COMPOSE) exec -T postgres psql -U $(or $(POSTGRES_USER),haweye) -d $(or $(CDC_PG_DB),dimensions) -c "SELECT count(*) AS cards FROM public.card_accounts"
	@echo ">>> now compare with the lakehouse:"
	./jobs/submit/run_job.sh cdc_merge_batch --dry-run

cdc-down:              ## Stop the CDC stack (keeps its volumes)
	$(COMPOSE_CDC) down

# ------------------------------------------------------------------------------
# 5. Airflow
# ------------------------------------------------------------------------------
airflow-up:            ## Airflow webserver -> http://localhost:8085 (admin/admin)
	$(COMPOSE) --profile airflow up -d

airflow-unpause:       ## Enable the schedules (DAGs ship paused=False but Airflow 2.8 pauses new files)
	$(COMPOSE) exec airflow-scheduler bash /opt/airflow/scripts/dag_ctl.sh unpause

airflow-train-now:     ## Trigger the nightly training DAG right now
	$(COMPOSE) exec airflow-scheduler airflow dags trigger fraud_model_training

airflow-pause-cdc:     ## The CDC DAG ships paused (needs `make cdc-up` first)
	$(COMPOSE) exec airflow-scheduler airflow dags unpause cdc_dimension_merge

# ------------------------------------------------------------------------------
# 6. table inspection / maintenance
# ------------------------------------------------------------------------------
sql:                   ## Interactive spark-sql against the lakehouse catalog
	$(COMPOSE) exec spark-master spark-sql --master 'local[*]' \
	  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

maintain:              ## Run compaction + snapshot expiry + orphan cleanup now
	./jobs/submit/run_job.sh table_maintenance --older-than-days 7

maintain-report:       ## Files / bytes / snapshots per table (no writes)
	./jobs/submit/run_job.sh table_maintenance --report-only

# ------------------------------------------------------------------------------
# 7. quality gates (run before every commit / in CI)
# ------------------------------------------------------------------------------
lint:                  ## ruff + compose/SQL/import validation + docs links + shell syntax
	$(PY) -m ruff check . --output-format concise
	$(PY) scripts/validate_compose.py
	$(PY) scripts/check_docs_links.py
	@for f in $$(find . -name '*.sh' -not -path './.venv/*' -not -path './artifacts/*'); do \
		bash -n "$$f" || { echo "syntax error in $$f"; exit 1; }; done
	@echo "shell scripts parse cleanly"

test:                  ## Unit tests: pure python + pandas, ~10s, no docker/java
	$(PY) -m pytest tests/unit -q

test-spark:            ## Spark-backed tests (needs java + pyspark, ~2 min)
	$(PY) -m pytest tests/integration -q

test-all: lint test    ## What CI runs

api-up:                ## Start only the serving API (http://localhost:8000/docs)
	$(COMPOSE) --profile api up -d serving-api

api-logs:              ## Tail the serving API logs
	$(COMPOSE) logs -f --tail=100 serving-api

api-smoke:             ## Score one hypothetical transaction via the REST API
	curl -s -X POST http://localhost:$(or $(API_PORT),8000)/v1/score \
	  -H 'content-type: application/json' \
	  -d '{"card_id":"CARD-00042","merchant_id":"MER-0007","amount":4200,"channel":"online","merchant_country":"BR","card_present":false}' | $(PY) -m json.tool

# ------------------------------------------------------------------------------
# 8. github for absolute beginners (docs/09-github-for-beginners.md)
# ------------------------------------------------------------------------------
pr:                    ## Stage, commit, push this branch and open a pull request
	./scripts/make_pr.sh

status:                ## Short git + stack status
	@git status --short --branch | head -20; echo; $(COMPOSE) ps --format 'table {{.Name}}\t{{.Status}}' 2>/dev/null | head -20

.PHONY: help bootstrap build up up-full down nuke ps logs check seed-dims data-backfill \
        data-backfill-small gen-stream gen-burst kafka-tail jobs-up jobs-down jobs-status \
        job-ingest job-features job-score train-model model-current model-versions \
        model-rollback cdc-up cdc-stream cdc-sync cdc-demo cdc-status cdc-check cdc-down \
        airflow-up airflow-unpause airflow-train-now airflow-pause-cdc sql maintain \
        maintain-report lint test test-spark test-all api-up api-logs api-smoke pr status
