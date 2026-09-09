# 00 — The beginner tour

*Read this first if any word in the README was unfamiliar. It is deliberately
long. Every tool gets the same three questions answered: what is it, why does
this project need it, and where in this repo do I see it.*

You do not need to memorise any of this. You need it once, so that when
something breaks at 11pm you know which box in the diagram to kick.

---

## 0. The one-sentence version

We invent a payments company. It produces card transactions one at a time. This
repo **catches them, remembers them, describes them, decides whether each one is
fraud, and lets a human review the decision** — all within seconds for the live
path, and all reproducible for the history.

```
   transactions            events                features              decisions
generator ──► Kafka ──► Spark Structured Streaming ──► Iceberg (on MinIO) ──► Redis + Postgres ──► REST API
  (fake)      (queue)        (compute)                    (the truth)           (the fast copy)      (answers)
                                   ▲                              ▲
                          Airflow ─┘  nightly training + maintenance
                          Debezium/CDC ─┘ dimension tables (merchants, cards) stay fresh
```

---

## 1. Git and GitHub (5 minutes, no prior knowledge)

**Git** is a camera for a folder. Every `git commit` stores *"the whole project
looked like this, at this time, and this person says why"*. It lives entirely on
your machine, in the hidden `.git/` folder inside the project.

**GitHub** is a server that holds a copy of that folder so others can see it, and
adds workflow around it (issues, pull requests, CI). You can use git forever
without GitHub; GitHub without git doesn't exist.

Five commands cover 95% of what you'll do here:

| command | what it means in plain words |
|---|---|
| `git status` | "what has changed since the last photo?" |
| `git add -A` | "put all changes in the box for the next photo" |
| `git commit -m "why"` | "take the photo, with a caption" |
| `git push` | "send my photos to GitHub" |
| `git switch -c my-branch` | "start an alternative timeline so I don't mess up main" |

A **branch** is just a moving label on a commit. `main` is the label everyone
agrees is "the good one". You work on your own label, then ask for it to be
merged via a **pull request** (PR): *"my branch is ready, review it"*. CI runs
`make lint` and `make test` on that PR; if it is green and a human approves, you
merge. `docs/09-github-for-beginners.md` walks through your first PR click by
click, including what to do when git says *"conflict"*.

> Why this project cares: every file you are about to read is versioned, and the
> CI config in `.github/workflows/` is what turns "I think it works" into "it
> works on a clean machine".

---

## 2. Docker: containers, images, volumes, compose

An **image** is a sealed folder with a program inside (plus its whole OS-ish
environment). A **container** is that image running as a process, with its own
private network and filesystem.

Why anyone bothers: Spark needs Java 8/11/17, a specific Scala, jars; Kafka needs
Java; Airflow needs Python + a database. Installing all of that on your laptop
*by hand* is a two-day job that breaks on the next macOS update. With Docker it
is `make up`.

Three concepts you must not skip:

* **image vs container** — `docker compose build` creates images;
  `docker compose up` runs containers from them. Editing `jobs/*.py` needs
  neither: the files are **mounted** in, so the container sees your edit.
  Editing a `Dockerfile` needs a rebuild.
* **volume** — a folder Docker keeps when containers die. Your Postgres tables and
  the Iceberg parquet files live in volumes: `make down` keeps them,
  `make nuke` deletes them. This is the difference between "restart" and "start
  over", and it's the first question to ask yourself when something feels broken.
* **the network** — inside compose, every service can reach every other by
  **service name**: `kafka:29092`, `postgres:5432`, `minio:9000`. Your laptop
  cannot use those names; it uses the *published* port (`localhost:9094`). The
  single most common beginner bug is using `localhost` inside a container. The
  compose files always use service names; `.env` always holds host names.

`docker compose ps` is the truth about what's running; `docker compose logs -f
<spanish>` is how you read its mind.

---

## 3. Kafka: the queue that decouples everyone

Kafka is a distributed, append-only log. Producers append, consumers read at
their own pace, and — the part that surprises people — **each consumer remembers
its own position** (an *offset*). Nothing is deleted when read.

* **topic** = a named log (`raw_transactions`).
* **partition** = a sub-log, the unit of parallelism. 3 partitions ⇒ up to 3
  consumers in one group reading in parallel.
* **key** = "records with the same key go to the same partition, in order". We
  key by `card_id`, so all activity for one card is sequential — which is exactly
  what "5 transactions in 5 minutes" requires.
* **consumer group** = one logical reader. Groups don't compete with each other;
  members within a group do.
* **retention** = how long data is kept. We keep 24h: Kafka is a *pipe*, not the
  warehouse. Iceberg is the warehouse.

Why Kafka instead of writing straight to Spark? Because a stream that can be
**replayed** is a stream you can reprocess after a bug fix, and a queue that
buffers means the payments system never waits for your analytics.

*Advertised listeners*: the same broker answers on `kafka:29092` (inside) and
`localhost:9094` (outside). This is a Kafka-specific trick worth understanding
once — see `docs/04-streaming.md#two-networks`.

---

## 4. Apache Spark: batch and streaming, same API

Spark computes on tables that don't fit in one process. Two objects matter:

* **SparkSession** — your connection to a cluster. `local[*]` means "use my cores,
  no cluster"; `spark://spark-master:7077` means "use the cluster in compose".
* **DataFrame** — a lazy table. `.filter()` and `.withColumn()` build a *plan*;
  nothing runs until an *action* (`.count()`, `.write`, `.collect()`).

Two consequences that explain everything about this repo:

1. **`collect()` to your laptop = the answer is small.** Never `collect()` a
   billion rows; that's what writing to a table is for.
2. **laziness means one definition can serve batch and streaming.** Our feature
   SQL in `jobs/common/features.py` is used by the streaming job *and* the
   backfill, unchanged.

**Structured Streaming** is Spark applying that DataFrame API to an
append-only stream. Each **trigger** (every 10s here) it processes the new rows
as one **micro-batch** — a normal batch — and writes them. That's the whole idea:
*streaming as repeated micro-batches*. The three things you must know:

* **checkpoint** = where it remembered it got to (in MinIO). Delete it and it
  re-reads from the beginning.
* **watermark** = "I stop waiting for data older than X". We use 2h so a late
  transaction still lands in the right window.
* **`foreachBatch`** = the escape hatch for anything a stream can't do: a
  `MERGE INTO` an Iceberg table, or writing to Postgres/Redis. Our three streaming
  jobs use it for exactly that.

---

## 5. Apache Iceberg: the table format that makes a data lake a database

Parquet is a *file* format: fast columns, no rules. Point Spark at a folder of
parquet files and you have... a folder. Iceberg adds a **metadata layer on top**:

* a **schema** with types (add a column without rewriting anything),
* **snapshots**: every commit is an immutable point-in-time version,
* **hidden partitioning**: you say "partition by `days(event_ts)`" once; queries
  get file pruning without users writing `WHERE dt='...'`,
* **row-level `MERGE`/`UPDATE`/`DELETE`** — the reason CDC is possible at all,
* **ACID commits** — readers never see a half-written batch,
* and it is **engine-agnostic**: Spark, Flink, Trino, DuckDB can read the same
  tables.

Iceberg stores two things: *data* (parquet, in MinIO under `warehouse/`) and
*catalog* (which snapshot is current — Postgres db `catalog`, schema
`iceberg_catalog`). Losing the catalog doesn't lose your data; it loses the
index to it.

Practical consequences in this repo:

* `time travel`: `SELECT * FROM lake.raw.transactions_enriched
  VERSION AS OF <snapshot_id>` — "what did the table look like at 10:04?".
* `MERGE INTO` gives us **exactly-once semantics on a queue that is
  at-least-once**: replay Kafka and the table is unchanged.
* The cost: streaming commits create *many* small snapshots and files, so
  maintenance (compaction, snapshot expiry) is not optional — that's
  `jobs/table_maintenance.py` + the nightly Airflow DAG.

---

## 6. MinIO: S3 on your laptop

S3 is the object storage API the industry standardised on. MinIO is that API,
self-hosted, in one container. Iceberg writes through the `s3a://` Hadoop
filesystem, so the only unusual thing you'll see here is
`path-style access = true` (bucket in the path, not in the hostname) — required by
MinIO, and it lives in `infra/config/spark/spark-defaults.conf`.

The bucket `lakehouse` holds `warehouse/` (Iceberg) and `models/` (trained
models). `make sql` then `DESCRIBE FORMATTED` shows you exactly which objects.

---

## 7. Postgres, Redis, and the "serving" split

A lakehouse answers analyst queries in seconds. A card authorisation must be
answered in **milliseconds**, thousands per second. So we split by workload:

* **Postgres** (`lakehouse` db) — the operational store. `serving.fraud_scores`,
  `serving.fraud_alerts`, feature mirrors. Durable, joinable, human-readable,
  queryable with `psql`. Written by Spark inside `foreachBatch`.
* **Redis** — the online feature store: `feat:txn:<id>`, `feat:card:<id>`,
  `score:txn:<id>` with a TTL. Sub-millisecond reads for the scorer. It's a
  **cache**: if it's empty, the API falls back to Postgres (see
  `api/serve.py:load_online_features`) — which is why "Redis down" is degraded,
  not fatal.
* **Postgres (`dimensions` db)** — the *source of truth* for merchants and cards
  (the business owns it), replicated **into** the lakehouse by CDC. Note the
  direction: dimensions flow *in*, facts flow *out*.

---

## 8. Airflow: the scheduler (and what it is *not*)

Airflow runs your batch jobs **when they should run**, retries them, backfills
them, and shows you a graph of what depends on what. It does *not* move data —
that's Spark.

* **DAG** = a Python file describing tasks and their order (`airflow/dags/`).
* **logical date (`ds`)** = the date a run *represents*. Templates use
  `{{ ds }}`; a run at 02:30 on the 3rd is about the 2nd. Getting this wrong is
  how you "retrain on today's partial data".
* **operator** = the thing a task does. We use `BashOperator` calling
  `run_job.sh` — deliberately, so a failed task can be reproduced by copy-pasting
  one line into a terminal.
* New DAGs start **paused** (`make airflow-unpause`).

Our DAGs: nightly `fraud_model_training` (with an AUC guardrail),
`lakehouse_maintenance` (compaction/expiry), `data_quality_checks` (every 15 min),
`cdc_dimension_merge` (the scheduled alternative to the CDC stream).

---

## 9. CDC and Debezium (the part you asked to add)

**CDC** = change data capture: instead of asking a database "give me everything"
every hour, you subscribe to its write-ahead log and get *each row change* as it
happens.

**Debezium** does that for Postgres, as a **Kafka Connect** plugin: it reads the
WAL (logical replication), and appends JSON events to topics
(`cdc.public.merchants`): `{"op":"u","before":{...},"after":{...},"source":{...}}`.

Why it matters here: features join against `dim.merchants`. With an hourly batch
load, a merchant whose risk score changes is stale for an hour — and a fraud
decision made against stale limits is wrong. With CDC, `jobs/cdc_merge_stream.py`
turns those events into an Iceberg `MERGE` in seconds, and *deletes* actually
delete.

What it needs from Postgres: `wal_level=logical`, a `PUBLICATION`,
`REPLICA IDENTITY FULL` (so updates carry the *old* row), and a role with
`REPLICATION`. All of that is in `cdc/sql/00_cdc_setup.sql`, explained line by
line, and the deep dive is `docs/05-cdc.md`.

Bonus: because Iceberg has snapshots, CDC gives you **point-in-time joins** —
"what did the merchant record say *when the transaction was scored*?". That is
how you debug a model without the labels lying to you.

---

## 10. NiFi (in the brief, optional here)

NiFi is a drag-and-drop data router: processors, queues between them, visible
backpressure, provenance. Excellent for real sources (bank APIs, SFTP drops,
webhooks). For a synthetic feed it adds ~6 minutes of clicking and nothing you
can test, so this repo defaults to `generator/` and keeps NiFi one flag away:
`docker compose --profile nifi up -d nifi`, then build the 3-processor graph described in [`../nifi/README.md`](../nifi/README.md).

---

## 11. The ML part, honestly

Fraud is a **badly imbalanced** problem: ~2–5% positives. Consequences:

* **accuracy is meaningless** (predicting "no fraud" always gets 96%). You watch
  **ROC-AUC**, **average precision**, and the number of alerts a human can work.
* **never split randomly.** Random splits leak (the same card appears before and
  after). We split by **time**: train on the past, test on the most recent
  `--holdout-days`. `jobs/train_model.py`.
* **never let the producer send labels to the stream.** The simulator keeps
  `label` off the wire; `tests/unit/test_generator_contract.py` asserts it. A
  feature that predicts fraud only because the generator leaked the answer is the
  oldest trap in this field.
* **features must be identical at training and scoring time.** One module
  (`jobs/common/features.py`) defines them; both paths use it; `VectorAssembler`
  is saved *inside* the pipeline so the order can't drift. The REST API reads the
  same feature JSON the job wrote.

---

## 12. Where to read code, in what order

1. `jobs/common/schema.py` — the contract (what a transaction *is*).
2. `jobs/streaming_ingestion.py` — the smallest real job: Kafka → parse →
   quarantine → Iceberg.
3. `jobs/common/features.py` — the definitions, and the comment explaining why
   24h windows come from an aggregate table.
4. `jobs/feature_store.py` — the write to Iceberg *and* Redis.
5. `jobs/real_time_scoring.py` — model + rules → decision → alerts.
6. `jobs/train_model.py` — time-based holdout, metrics, publish pointer.
7. `jobs/cdc_merge_stream.py` + `jobs/common/cdc.py` — the CDC path.
8. `api/serve.py` — what all of it was for.

Each one starts with a docstring that says *why this file exists* — read those
before the code, they're shorter than you'd expect.

---

## 13. Words you'll see and might not know

| word | meaning here |
|---|---|
| idempotent | running it twice is the same as once (MERGE + dedupe key make our writes idempotent) |
| exactly-once | the effect happens once even though delivery is at-least-once |
| backfill | compute history you missed, into the same tables |
| upsert | insert, or update if the key exists |
| late arrival | an event whose time is older than where the stream already is |
| compaction | rewrite many small files into a few big ones |
| schema drift | producer added/renamed a column |
| poison pill | one bad record that stops a stream (we quarantine instead) |
| TTL | time to live; Redis expires our feature keys after 24h |
| label leakage | a feature that encodes the answer |
| cold start | no trained model yet; rules carry the decision (see `rules.blend`) |

Next: **[02-first-run.md](02-first-run.md)** — actually do it.
