# 02 — Your first run, one command at a time

*Assumes: a laptop with 8+ GB RAM, and no prior Docker/Spark experience. Every
step shows what you should see. If you don't see it, the "if not" line is your
next action — don't skip ahead.*

Total: ~25 minutes the first time (mostly image downloads).

---

## Step 0 — install two things

**Git** and **Docker Desktop** (Mac/Windows) or **Docker Engine + compose plugin**
(Linux). On Ubuntu, as your own user (this is exactly what
`docs/08-setup-macos-linux-windows.md` covers per OS):

```bash
sudo apt-get update
sudo apt-get install -y git ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
  https://download.docker.com/linux/ubuntu stable" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update && sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER   # log out and back in, or: newgrp docker
docker run --rm hello world     # ← the one test that matters
```

On macOS just `brew install --cask docker git` and open Docker once. **Give Docker
8 GB** (Docker Desktop → Resources) — Spark + Kafka + MinIO + Airflow in 4 GB
will die in ways that look like code bugs.

## Step 1 — get the code

```bash
git clone https://github.com/Sanjaytemp/Haweye.git
cd Haweye
```

`git clone` copies the *whole history*, not just the files — which is why you can
`git log` immediately. New to this? `docs/09-github-for-beginners.md`.

## Step 2 — the bootstrap

```bash
make bootstrap
```

It checks the prerequisites, copies `.env.example` → `.env`, warns you about port
conflicts, makes a `.venv` with the tooling `make lint`/`make test` need, runs
`scripts/validate_compose.py` (YAML, depends_on, env vars, every generated SQL
string parsed), and builds the images.

You should see, ending with:

```
>>> all good: docker, compose, ports, .env, config validation
Build complete. Next:  make up
```

**If not**: fix whatever it printed. A "port 5432 already in use" line means a
Postgres is already running on your machine — change the host port in `.env`
(`POSTGRES_PORT=5433`) rather than killing your database.

## Step 3 — start the platform

```bash
make up
```

Seven containers come up: kafka, minio, postgres, redis, spark-master,
spark-worker, plus the one-shot topic/initialiser jobs.

```bash
make ps          # every service should say "healthy" (or "exited (0)" for one-shots)
make check       # scripts/check_env.sh: reachable + configured, with fix hints
```

`make check` is the tool to reach for all day. It ends with:

```
PASS  kafka          3 topics, broker up (advertised 9094/29092)
PASS  minio          bucket lakehouse present
PASS  postgres       databases catalog/lakehouse/airflow, wal_level=logical
PASS  redis          PONG
PASS  spark          master answering on 8080, 1 worker registered
0 FAIL
```

**If Spark says "0 workers"**: `docker compose logs spark-worker` — usually it's
memory (see Step 0).

## Step 4 — dimensions (the tables CDC will replicate)

```bash
make seed-dims
```

Creates `public.merchants` (120 rows) and `public.card_accounts` (200) in the
`lakehouse` database, with the `updated_at` trigger CDC relies on. Verify:

```bash
docker compose exec -T postgres psql -U haweye -d lakehouse \
  -c "select count(*) from public.merchants"
```

Why seed them at all? So the feature job's joins find rows. With no dimensions,
every enriched `merchant_risk_score` is NULL and the model learns nothing — a
lesson you can *watch* by skipping this step once.

## Step 5 — history, so training has something to read

```bash
make data-backfill-small       # 3 days × 1500 rows (laptop-friendly)
```

This runs `jobs/backfill_training_data.py` inside the Spark cluster and prints, per
table, the rows written. Expect:

```
>>> raw.transactions_enriched      rows=4500
>>> features.transactions_feature_v1 rows=4500
>>> raw.fraud_labels               rows=4500
```

Then look at it with real SQL:

```bash
make sql
spark-sql> select count(*), min(event_ts_ts), max(event_ts_ts) from lake.raw.transactions_enriched;
spark-sql> select amount, channel, merchant_country, txn_count_5min, amount_zscore_24h
           from lake.features.transactions_feature_v1 order by event_ts_ts desc limit 5;
spark-sql> describe history lake.raw.transactions_enriched;   -- ← Iceberg's own log
```

`DESCRIBE HISTORY` is the moment Iceberg clicks: each row is a commit you can
time-travel to.

## Step 6 — the live stream (3 terminals, or one background)

Start the three jobs, then produce traffic:

```bash
make jobs-up          # ingestion, features, scoring — detached, logs to .run/*.log
make gen-stream       # 25 transactions/sec into Kafka (Ctrl-C stops it)
```

Watch it work:

```bash
make logs                                   # all containers
tail -f .run/streaming_ingestion.log          # "rows=250, quarantined=0" every ~10s
make kafka-tail                               # the raw topic, before Spark touches it
```

You should see each job advance: rows into `raw`, then features into
`features` + Redis, then decisions into Postgres:

```bash
docker compose exec -T postgres psql -U haweye -d lakehouse -c \
  "select decision, count(*), round(avg(final_score),3) avg_score
     from serving.fraud_scores group by 1 order by 2 desc"
```

`monitor/approve/review/decline` counts that all say `approve` means the fraud
rate is too low for the window — generate a burst:

```bash
make gen-burst        # 90% fraud for a couple of minutes
```

Now `decline` and `serving.fraud_alerts` move. That single experiment teaches more
than an hour of reading.

## Step 7 — train the model

```bash
make train-model
```

Output that matters:

```
rows=5400 train=4500 test=900 positives(test)=41
auc=0.91  ap=0.62  f1=0.58  threshold=0.6
top features: amount_zscore_24h, txn_count_5min, country_mismatch, ...
>>> published v20260909T090512Z (auc=0.91)
```

* `auc` between ~0.85 and 0.95 is what the simulator's noisy labels support.
  **1.0 means you leaked the label** — check nothing added `label` to the wire
  payload.
* `train`/`test` are *time-split*: the test window is the most recent days.

```bash
make model-current      # which version is live
make model-versions     # what's in the registry
```

The next scoring micro-batch picks it up by itself (it re-reads `version.txt`).
No restart — that's the deploy story in `docs/07-ml.md`.

## Step 8 — the API: what it was all for

```bash
docker compose --profile api up -d serving-api
make api-smoke
```

```json
{
  "transaction_id": "HYP-20260909090909",
  "score": 0.94,
  "decision": "decline",
  "model_score": 0.96,
  "rule_score": 0.87,
  "rule_hits": ["EXTREME_TICKET", "CNP_ABROAD", "AMOUNT_SPIKE_1H"],
  "features_used": { "txn_count_5min": 6, "amount_sum_1h": 9120.0, "__source": "redis:feat:card:CARD-00042" },
  "model_version": "v20260909T090512Z",
  "latency_ms": 3.1
}
```

Open <http://localhost:8000/docs> — every endpoint is interactive, including
`GET /v1/features/{id}` (the exact vector the model saw) and
`PATCH /v1/alerts/{id}` (work the queue).

`__source` tells you whether it came from Redis or Postgres. If it says `none`,
Redis has no entry for that card — features expired (TTL) or the job isn't
writing. `make api-logs` and check job 2.

## Step 9 — Airflow (the scheduler)

```bash
make airflow-up
# wait ~60s, then open http://localhost:8085 (admin / admin)
make airflow-unpause
make airflow-train-now
```

In the UI, click a run → the `train_model` task → **Log**. The log line is the
same command you typed in step 7. That's on purpose: a task that can't be
reproduced in a terminal is a task you can't debug.

## Step 10 — CDC, the thing you asked to add

```bash
make cdc-up        # extra Postgres (wal_level=logical) + its own Kafka + Connect + connector
make cdc-demo      # 5 rounds of dimension INSERT/UPDATE/DELETE, printed as SQL
make cdc-sync      # one merge: Kafka CDC topic → Iceberg MERGE
make cdc-status    # connector state, tasks, how many records
make cdc-check     # row counts: source vs lakehouse
```

Expected, in order: connector `RUNNING`, `records_filter=0`, then `cdc-check`
printing matching counts. Now the payoff:

```bash
make sql
spark-sql> select * from lake.dim.merchants order by source_ts_ms desc limit 5;
spark-sql> select * from lake.dim.merchants
           for version as of (select snapshot_id from lake.dim.merchants.snapshots limit 1);
```

You have just replicated a database table into a lakehouse, watched deletes
delete, and time-travelled to the state the model saw.

## Step 11 — make it yours

```bash
git switch -c my-first-change
# edit: add a rule to jobs/common/rules.py (both `sql` and `python`)
make test           # the parity tests will tell you what you missed
make lint
./scripts/make_pr.sh
```

`scripts/make_pr.sh` runs the gates, pushes your branch, and opens the PR
(`docs/09-github-for-beginners.md` explains each of those three things).

## What to break on purpose (best 20 minutes of learning here)

| do this | you will see |
|---|---|
| `docker compose stop redis` then `make api-smoke` | `__source: "postgres:card"` — the fallback works, API is degraded not down |
| `docker compose exec kafka kafka-console-producer --topic raw_transactions --bootstrap-server localhost:9092` then type `garbage` | a row in `raw.load_failures` with reasons; the stream keeps going |
| delete the Redis keys: `docker compose exec redis redis-cli flushall` | features vanish; scoring still runs, model_score present, rules weaker |
| `make nuke && make up` and skip step 4 | `country_mismatch`/`merchant_risk_score` all NULL — the join found nothing |
| set `GEN_FRAUD_RATE=0` and retrain | AUC collapses to ~0.5 — nothing to learn; the guardrail refuses to publish |
| `rm -f .run/*.pid; make jobs-status` | the truth about what is alive (a pid file is not a process) |

Then `make down` (keeps data) or `make nuke` (fresh start). Both are safe: nothing
here matters but your own data.
