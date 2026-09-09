# 04 — Structured Streaming: the semantics this repo depends on

*Everything here is visible in `jobs/common/sparkutils.py` and the three job
files. No new ideas beyond what the code does.*

## The mental model, in four sentences

A streaming query is a batch job that runs forever. On each **trigger** it takes
whatever is *new since the last time* (for Kafka: the offsets after the ones in
its checkpoint), runs the same DataFrame plan, and hands the result to a **sink**.
If it crashes mid-batch, the batch is replayed — so the *sink* is what has to be
idempotent, not the transform.

That's why every write in this repo is either `append` to an immutable table, or
`MERGE … WHEN NOT MATCHED` on a natural key.

## Triggers

```python
sparkutils.trigger()                 # {"processingTime": "10 seconds"}
sparkutils.trigger(once=True)       # {"once": True}
```

`ProcessingTime("10 seconds")` = "at most every 10s, and never less" — a batch
that takes 40s does not queue up four more. `Once` runs until the input is
exhausted, then stops: that is what `make job-ingest --once` and the Airflow batch
jobs use, and it is the reason the same job file works in both modes.

Spark 3.5's `DataStreamWriter.trigger()` is **keyword-only** (`Trigger` objects
were removed from `pyspark.sql.streaming`), which is why `trigger()` here returns
a dict of kwargs instead of a `Trigger`.

## Checkpoints: two directories, not one

```
s3a://lakehouse/warehouse/checkpoints/<job>/     offsets + state  (Spark writes)
s3a://lakehouse/warehouse/…                      data              (Iceberg writes)
```

The checkpoint is the job's *memory*. Consequences people discover the hard way:

* delete it and `starting=latest` means "start from Kafka's tail" (you skip
  history), `starting=earliest` means "re-read everything Kafka still has"
  (duplicates — harmless here *because* writes are MERGED);
* moving a query's checkpoint to another table's checkpoint = corrupt state;
* `--once` uses the *same* checkpoint, so a `--once` run advances the position of
  the 24/7 job. That is intended (it's how you catch up) but be aware.

One line in `start_query` matters: `failOnDataLoss=false` in
`sparkutils.kafka_source`. Kafka deleted old segments? The job skips ahead
instead of dying forever. In production you pair that with an alert on
`skipped records`, not silence.

### <a name="checkpoint-vs-overwrite"></a>Checkpoint vs overwrite (the classic)

| symptom | cause | fix |
|---|---|---|
| numbers double after a restart | sink uses `outputMode("complete")`/`overwrite` on an append-only table | MERGE on a key (what `merge_batch` does) |
| same rows twice in Iceberg | `starting=earliest` + no dedupe key | `dedupe()` per batch + `WHEN NOT MATCHED` across batches |
| job "replays from the beginning" every start | checkpoint path wrong/unwritable | `make check` → the MinIO path; check creds |
| new file in checkpoint dir but no data written | query died during commit | Iceberg has no half-commit; the replay is safe |

## Watermarks and late data

We use event time everywhere (`event_ts_ts`), never ingest time, because a
transaction at 10:00:00 must be counted in the 10:00 window even if it arrives at
10:00:40. `STREAM_WATERMARK_HOURS=2` says: keep the ability to correct a window for
2 hours, then close it.

Where it actually bites: the **24h features do not use a Spark window at all** —
they read `raw.card_day_agg` (see next doc). Watermarks therefore protect the
5min/1h windows and the `--verify` parity check, not the day aggregates. That is
a deliberate trade: a 24-hour window over a stream is state you must keep in
memory/state-store for 24 hours; a daily aggregate table is 1/8640th of that.

## <a name="reading-iceberg-as-a-stream"></a>Reading Iceberg as a stream

`sparkutils.iceberg_source()` (job 2 reads the enriched table this way) is
*append-only* by design in Iceberg 1.5:

```python
# jobs/common/sparkutils.py, simplified
reader = (spark.readStream.format("iceberg")
          .option("streaming-skip-overwrite-snapshots", "true")
          .option("streaming-skip-delete-snapshots", "true")
          .option("stream-from-snapshot-id", current_snapshot_id(spark, table)))  # "latest"
```

Real options (Iceberg 1.5.2 docs), and what they cost you:

| option | meaning |
|---|---|
| `stream-from-snapshot-id` | start *after* this snapshot; we resolve "latest" to the current snapshot id |
| `stream-from-timestamp` | start after a wall-clock ms (used by `--starting timestamp:…`) |
| `streaming-skip-overwrite-snapshots` | **required here**: a `MERGE`/backfill commits overwrite snapshots; without it the stream throws and the job dies |
| `streaming-skip-delete-snapshots` | same for deletes/compaction output |
| `streaming-max-rows-per-micro-batch` | back-pressure when you are catching up on a big backlog |

Read that twice: **because we skip overwrite and delete snapshots, the stream only
sees pure appends.** Job 1 therefore `append`s to `raw.transactions_enriched` and
only `MERGE`s the *raw* table (dedupe) — the pipeline is shaped around this Iceberg
limitation on purpose, and job 2's writes are what make re-processing safe.
If you change job 1 to MERGE into the enriched table, job 2 stops seeing rows and
nothing will look like an error. That is the #1 "features stopped" bug in this
project.

## foreachBatch: where the real work is

```python
writer.foreachBatch(lambda micro, batch_id: sink(spark, micro, batch_id))
```

Inside that function you may run *batch* code on the micro-batch: `MERGE INTO`,
a JDBC upsert, Redis pipeline writes. Two rules that keep you out of trouble:

1. **it must be safe to re-run** — every write is keyed (dedup_key /
   transaction_id / merchant_id);
2. **don't read a bounded table inside the transform** (`mapInPandas` reading the
   whole history would be re-read per partition, and Spark forbids mixing
   unbounded streams with batch reads in most operators). That's exactly why the
   aggregate tables exist and why enrichment/feature computation happens in
   `foreachBatch`.

`process_batch(spark, micro, batch_id)` in each job file is that function,
extracted to module level so `tests/integration/test_pyspark_sql.py` can call it
without any streaming plumbing.

## Lag, back-pressure, and sizing

Kafka consumer lag is the metric. `make kafka-lag` (or the `kafka_lag` task in the
quality DAG) shows it. If lag grows steadily:

* more partitions on `raw_transactions` (3 is a teaching default; partitions are
  the parallelism ceiling for readers),
* `STREAM_MAX_ROWS_PER_TRIGGER` / `maxOffsetsPerTrigger` to keep batches flat,
* `spark.sql.shuffle.partitions` (4 in `get_spark` for laptops — raise it on a
  real cluster),
* or a longer trigger interval. Latency targets are a *choice*, and 10s is not
  sacred.

## Stop it politely

`install_graceful_stop(query)` registers SIGTERM/SIGINT → `query.stop()`, which
finishes the in-flight micro-batch instead of killing it mid-commit. That is why
`make jobs-down` (SIGTERM) is correct and `docker compose kill` (SIGKILL) is
sometimes not: the latter can leave the checkpoint written for batch N while
batch N+1's Iceberg commit never happened — safe here (idempotent MERGE), but you
will see a duplicated-looking replay in the logs and waste 20 minutes on it.

## What "exactly once" means here

Spark's end-to-end exactly-once needs a transactional sink it controls (Kafka
transactional writes with the checkpoint in the same transaction). We don't use it
— the honest label for this pipeline is:

> **at-least-once delivery, exactly-once *effect*** for the Iceberg tables
> (keyed MERGE), and **idempotent-but-not-transactional** for Redis/Postgres,
> which are caches/projections that get rewritten by the next batch anyway.

If you need Postgres to be exactly-once too, add a batch-id ledger table
(`INSERT … ON CONFLICT DO NOTHING` on `(job, batch_id)`) and skip the write if the
row already exists. Ten lines; a good first PR.
