# 03 — Iceberg in this project

*The lakehouse isn't a product you install; it's a table format you configure.
This doc is what that sentence means, using our actual tables.*

## What we asked Iceberg to do for us

| requirement | Iceberg feature | where |
|---|---|---|
| 10s commits, readers never see partial data | atomic snapshot commit | `jobs/common/sparkutils.py:append_batch` |
| Kafka replay must not double-count | `MERGE … WHEN NOT MATCHED` on `transaction_id` | `streaming_ingestion.py` |
| CDC updates + deletes on a data lake | row-level `MERGE INTO` + `DELETE` | `jobs/common/cdc.py:merge_changes` |
| "what did the merchant record say at 10:04?" | snapshots + `FOR VERSION AS OF` | `docs/05-cdc.md#point-in-time-joins` |
| day-level file pruning | partitioning on a `dt date` column (`PARTITIONED BY (dt)`) | `jobs/streaming_ingestion.py` DDL |
| add a feature without rewriting history | schema evolution | `ensure_feature_columns` + `ALTER TABLE ADD COLUMNS` |
| millions of tiny files don't kill queries | `rewrite_data_files` | `jobs/table_maintenance.py` |

## The layout, physically

```
minio://lakehouse/warehouse/
  raw/transactions_enriched/     metadata/*.avro, snap-*.parquet, data/dt=2024-06-01/*.parquet
  raw/transactions_raw/          (append-only, no merge: the "as-received" ledger)
  raw/fraud_labels/
  raw/load_failures/             quarantine
  raw/card_minute_agg/           pre-aggregated history (makes 24h features cheap)
  raw/card_day_agg/
  raw/cdc_snapshot_state/        which CDC snapshot we last applied
  features/transactions_feature_v1/
  dim/merchants/  dim/card_accounts/          ← written by CDC only
  marts/data_quality_runs/
postgres://…/catalog.iceberg_catalog          ← TableEntry rows: name -> metadata-location
```

Two rules of thumb: **the warehouse path is where data lives; the catalog is
where "current" is defined.** A `SHOW TABLES IN lake.raw` reads Postgres, not S3.

## Snapshots: the feature to actually practice

```sql
-- every commit, with what it did
SELECT version, snapshot_id, parent_id, timestamp, operation,
       summary.'added-data-files' AS added, summary.'total-data-files' AS files
FROM lake.raw.transactions_enriched.snapshots ORDER BY 1 DESC LIMIT 20;

-- time travel, three ways
SELECT count(*) FROM lake.raw.transactions_enriched FOR VERSION AS OF 8123456789;
SELECT count(*) FROM lake.raw.transactions_enriched FOR SYSTEM_VERSION AS OF '2024-06-01 10:04:00';

-- diff two snapshots: which rows appeared?
SELECT * FROM lake.table_changes('lake.dim.merchants', 8123456780, 8123456789);

-- the files themselves (how bad is the small-file problem, right now?)
SELECT * FROM lake.raw.transactions_enriched.files;
SELECT * FROM lake.raw.transactions_enriched.manifests;
```

`operations` is what to read when something "vanished": `append`, `overwrite`,
`delete`, `replace` (that's compaction), `merge`. If you see `overwrite` where you
expected `append`, a job lost its idempotency (a real class of bug — see
`docs/04-streaming.md#checkpoint-vs-overwrite`).

## Partitioning: what we chose and why

Facts are partitioned by an explicit `dt date` column that every writer sets from
`event_ts_ts` (`to_date(event_ts_ts)`). Two consequences: a query filtered on
`dt = '2024-06-01'` or on `event_ts_ts >= … AND < …` prunes files, and a row whose
event date is corrected later lands in the *new* partition (Iceberg has no
"move the row" — you delete + insert).

The alternative, `PARTITIONED BY days(event_ts_ts)`, is "hidden partitioning":
users never see or write a `dt` column and pruning works from the raw timestamp
expression alone. We keep `dt` because the serving mirrors, the backfill's
partition filter and the quality checks all want to say `dt =` explicitly. If you
prefer the purist version, change the DDL and drop `dt` from
`FEATURES_TABLE_SCHEMA` in one PR — it is a *breaking* layout change, so old
partitions stay where they are until you rewrite them.

Dimensions are **not** partitioned at all: two small tables, always read whole;
partitioning them would only multiply files.

Bucketing/`write.distribution-mode` is deliberately not used: with 10-second
commits, any hash distribution multiplies tiny files. That's a decision to
revisit only if the feature table grows past ~100M rows/day.

## Table properties we set (`jobs/common/table_props.py`)

These are the properties the code actually sets — grouped per table *role*, which
is why the module is a dict-of-dicts and not one global blob:

| table group | properties | why |
|---|---|---|
| `RAW` (facts) | `write.format.default=parquet`, `write.distribution-mode=hash`, `write.target-file-size-bytes=134217728` | columnar scans for training; hash distribution = fewer, larger files per partition |
| `FEATURES` | same as RAW + `read.parquet.vectorization.batch-size=1024` | the feature table is point-read by the backfill/API fallback |
| `DIMENSION` | `…-mode=none`, `target-file-size=67108864` (64 MB) | tiny tables, always read whole: a shuffle would cost more than it saves |
| `STATE` (CDC snapshot cursor) | `none`, 64 MB | one row per table; correctness, not throughput |
| `LABELS` | `parquet`, `hash` | appended by the backfill, scanned in full by training |
| `QUARANTINE` | `parquet`, `none` | read rarely, row by row |

Not set here, worth knowing: `write.parquet.compression-codec` (Iceberg's default
is already `zstd`), and `write.update.mode` / `write.delete.mode` (Iceberg's
default is **copy-on-write**). The DDL also sets no `format-version`, so tables
are v1 — `UPDATE`/`DELETE` work, position deletes do not.

Copy-on-write vs merge-on-read is the one Iceberg choice that bites people:
**COW rewrites the whole file for a single-row update** (great for reads, poor for
high-frequency updates to big files), **MOR writes delete files and merges at
read time** (great for update volume, adds read cost). Our dimension tables are
tiny and read often → COW is right. If you ever CDC a 500M-row table under
constant updates, switch that one table to MOR (needs `format-version=2`):

```sql
ALTER TABLE lake.dim.merchants SET TBLPROPERTIES ('write.update.mode'='merge-on-read',
                                                  'write.delete.mode'='merge-on-read');
```

## Why maintenance is not optional here

Streaming = 8,640 commits/day = 8,640 snapshots and hundreds of thousands of
files if left alone. `jobs/table_maintenance.py` (nightly, `make maintain`):

1. `rewrite_data_files` — bin-pack (`sort_by` optional; we don't sort, because
   our queries filter by time only and time is already the partition).
2. `expire_snapshots(retain_last=>?, older_than=>now()-7 days)` — drops history
   beyond the replay window. **This is what actually deletes data files.**
3. `remove_orphan_files(older_than=>now()-3 days)` — parquet no snapshot
   references (failed/aborted commits). The 3-day guard exists because an orphan
   check on a live table can delete a file a concurrent commit just wrote but has
   not published metadata for yet.
4. `analyze table ... for all columns` (opt-in `--analyze`) — column stats for the
   optimizer.

Run the report before and after to see it work:

```bash
make maintain-report   # files, bytes, snapshots per table
make maintain
make maintain-report
```

## Schema evolution: what is automatic and what is not

Automatic: the **enriched** and **feature** tables are created from
`FEATURES_TABLE_SCHEMA` / `_enriched_ddl()` on first write
(`sparkutils.ensure_table`), so adding a column *to the code* and starting with a
fresh volume gives you the new layout with no DDL at all.

Not automatic: an **existing** table never grows by itself — Spark refuses to
write a DataFrame with a column the table lacks. You have to say so, once:

```sql
ALTER TABLE lake.features.transactions_feature_v1
  ADD COLUMNS (amount_to_avg_5min double);
```

Then re-run the pipeline (new rows fill it), then `make data-backfill` (old rows
get it), then `make train-model` (the model must be retrained: the vector length
changed). The order matters: training before backfill silently trains with NULL →
0 for the new feature, which looks like "the new feature is useless".

`ensure_feature_columns` is what keeps *consumers* honest: it materialises every
`model_feature_names()` entry from whatever columns exist (missing → 0), and the
trained metadata records the exact feature list. If the pipeline's
`VectorAssembler` cannot assemble the vector anyway, scoring **fails safe**: the
job's `score_with_model` returns `model_score = NULL` with
`status="transform_failed:…"`, the rules still decide, and the alerts keep
flowing. A model that cannot load must never take the pipeline down.

Renaming or deleting a column that a published model uses is a breaking change:
ship a new `model_version` (new table or new `_v2` suffix), do not edit history.

## Files worth reading

* `jobs/common/sparkutils.py` — `append_batch`, `merge_batch`, `table_exists`,
  `snapshot_summary`: every Iceberg touch point.
* `jobs/common/table_props.py` — the property table above, as code.
* `jobs/table_maintenance.py` — the four steps, with the dry-run flag.
* `jobs/common/dimensions.py` — one column spec → both the Postgres and the
  Iceberg DDL (this is why they cannot drift).
