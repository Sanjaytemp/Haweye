# 05 — CDC with Debezium + Kafka Connect, merged into Iceberg

*What you asked for ("can we add CDC?"), implemented, and why each config line
exists. Start with `make cdc-up`, then read this while the connector catches up.*

## The problem in one paragraph

`features` joins against `dim.merchants` and `dim.card_accounts`. Those tables are
owned by the **business** (Postgres), not by the lakehouse. An hourly `JDBC` batch
load means: a merchant that just got flagged closed keeps transacting for up to an
hour; a card whose limit was lowered still "passes" the limit rule; and a
`DELETE` (a merchant that never should have existed) never propagates at all,
because `INSERT OVERWRITE` of a *snapshot* only reflects what exists at that
instant and you paid a full-table read for it.

CDC inverts it: Postgres tells you about every row change *as it happens*, and you
apply it to Iceberg with `MERGE`.

## The moving parts

```
Postgres (wal_level=logical)                Kafka Connect (Debezium connector)
  public.merchants                            connector: haweye-dimensions
  public.card_accounts                          ├ reads the WAL via publication
  PUBLICATION haweye_cdc                        └ writes topics cdc.public.*
  REPLICA IDENTITY FULL                                   │
                                                          ▼
                                     jobs/cdc_merge_stream.py  (24/7, 5s latency)
                                     jobs/cdc_merge_batch.py   (Airflow, 5 min)
                                          parse → latest-per-key → MERGE INTO
                                                          │
                                    lake.dim.merchants ───┴── lake.dim.card_accounts
                                    + serving.*_lakehouse mirrors in Postgres (for the API)
```

Three containers do the replication (`docker-compose.cdc.yml`): a **second**
Postgres, its **own** Kafka, and **Kafka Connect**. Why not reuse the main stack?
Because then a `make nuke` of the demo data would delete the connector's *offsets
topic* and its schema history, and the WAL slot on the source would keep growing
with no consumer — a "why is the database disk full" incident. Separating them
makes the failure domains match the mental model. You can point the connector at
the main Kafka by setting `CDC_KAFKA_SERVERS=kafka:29092` in `.env` if you want
one cluster.

## What Postgres needs, line by line

`cdc/sql/00_cdc_setup.sql` (run automatically by the container on first boot):

| thing | why it is not optional |
|---|---|
| `wal_level = logical` | the default (`replica`) WAL doesn't contain enough to rebuild rows. Setting this requires a **restart**; it cannot be changed on a live server. |
| `max_replication_slots`, `max_wal_senders` | a slot is a cursor; without headroom the connector can't start. |
| `CREATE PUBLICATION haweye_cdc FOR TABLE …` | logical decoding is *opt-in per table*; the publication is the opt-in. |
| `REPLICA IDENTITY FULL` | without it an `UPDATE` event carries only the **key** of the old row, no `before` image — so you cannot compute "what changed", and a `DELETE` keyed on a non-identity column is impossible. Our tables' PKs are surrogate ids, so we want FULL. |
| `CREATE ROLE debezium … REPLICATION` | a dedicated login for the connector: it should be able to read the WAL, not write the tables. |

And what the *connector* needs, in `cdc/connectors/haweye-dimensions.json`
(generated from `jobs/common/cdc.py:debezium_connector_config` — one source of
truth, so the file and the code cannot drift):

```
plugin.name = pgoutput            → uses Postgres' built-in decoder; no extension to install
publication.name = haweye_cdc     → must match the SQL above, exactly
slot.name = haweye_cdc_slot       → the WAL cursor; created by the connector, NOT by us
snapshot.mode = initial           → one consistent read of existing rows, then follow the WAL
tombstones.on.delete = false      → deletes arrive as op=d rows, not as null values
decimal.handling.mode = double    → numeric dimensions become JSON numbers, not base64 blobs
key/value.converter = JsonConverter (schemas off) → {"payload":{…}} — see the note below
```

Two of those are the ones people get wrong:

* **StringConverter on the key looks harmless and is not.** A relational table's
  key is a Struct; `StringConverter` throws "Converter … does not handle map
  values" on the *first captured row*. (It is only correct for the Debezium
  *outbox* pattern, where the key really is a string.)
* **`schemas.enable=false`** gives `{"payload": {…}}` instead of a 3×-larger
  envelope with an inline schema. `parse_cdc_stream` handles both, so you can
  flip it — but your downstream consumers should not have to.

## The event, and how we flatten it

```json
{"payload":{"op":"u","ts_ms":1717200000000,
  "source":{"db":"dimensions","schema":"public","table":"merchants","lsn":"0/1A2B3C","snapshot":"false"},
  "before":{"merchant_id":"MER-0007","merchant_risk_score":0.35,"updated_at":"..."},
  "after" :{"merchant_id":"MER-0007","merchant_risk_score":0.92,"updated_at":"..."}}}
```

`jobs/common/cdc.py:parse_cdc_stream` turns that into columns
(`op`, `source_table`, `source_ts_ms`, `before_json`, `after_json`, `is_delete`,
`ingest_ts`), `latest_change_per_key` keeps one event per entity per batch (ties:
**delete wins** — if you both update and delete in the same second, the row must
go away), and `merge_changes` issues:

```sql
MERGE INTO lake.dim.merchants t USING cdc_changes s ON t.merchant_id = s.merchant_id
WHEN MATCHED AND s.op = 'DELETE' AND s.source_ts_ms >= coalesce(t.source_ts_ms,0) THEN DELETE
WHEN MATCHED AND s.op <> 'DELETE' AND s.source_ts_ms >= coalesce(t.source_ts_ms,0) THEN UPDATE SET …
WHEN NOT MATCHED AND s.op <> 'DELETE' THEN INSERT (…, source_ts, source_ts_ms, op) VALUES (…)
```

The `source_ts_ms >=` gates are the whole trick: they make the merge **order-safe
and replay-safe**. Stream the topic from offset 0 and the table ends up identical;
events that arrive out of order cannot overwrite newer state. `MANAGED_COLUMNS`
(`source_ts`, `source_ts_ms`, `op`, `source_db`, `source_table`, `ingest_ts`) are
never treated as business data — that's what `target_data_columns()` filters out,
which is also why adding a column to the Postgres table is enough for the next run
to pick it up.

## Streaming vs scheduled: pick one

| | `cdc_merge_stream.py` | `cdc_merge_batch.py` (+ Airflow `*/5 * * * *`) |
|---|---|---|
| freshness | ~5s | up to 5 min |
| cost while idle | a Spark job stays running | nothing |
| retries/backfill | you write them | Airflow gives them |
| good for | 2 dimensions, real-time features | many tables, or a team that already runs Airflow |

`make cdc-stream` vs `make cdc-sync`. They are the same `apply_changes` code
(`jobs/common/cdc_merge_common.py`) — one function, two wrappers — so you can
switch without changing semantics.

## <a name="point-in-time-joins"></a>Time travel: the bonus nobody expects

Because Iceberg keeps snapshots, you can ask what the dimension said **at the
moment of the decision** — the difference between debugging a model with correct
context and with none. Note the shape of the query: time travel takes a
*literal* snapshot id or timestamp (you cannot time-travel per row), so you first
find the snapshot that was current at the decision, then query it.

```sql
-- 1) every version of the dimension table, newest first
SELECT snapshot_id, committed_at, operation, summary
FROM lake.dim.merchants.snapshots ORDER BY committed_at DESC LIMIT 10;

-- 2) the snapshot that was current at 10:00:05 on the day in question
SELECT max(snapshot_id) AS snap FROM lake.dim.merchants.snapshots
WHERE committed_at <= TIMESTAMP '2024-06-01 10:00:05';

-- 3) what the feature job joined, using that id
SELECT m.merchant_id, m.merchant_risk_score, m.merchant_closed, m.source_ts_ms
FROM lake.dim.merchants FOR VERSION AS OF 8123456789 m
WHERE m.merchant_id = 'MER-0007';

-- and today's view, for comparison: what changed between the decision and now
SELECT now.merchant_risk_score AS at_decision, then_.merchant_risk_score AS now_score
FROM (SELECT merchant_risk_score FROM lake.dim.merchants FOR VERSION AS OF 8123456789 WHERE merchant_id='MER-0007') now,
     (SELECT merchant_risk_score FROM lake.dim.merchants WHERE merchant_id='MER-0007') then_;
```

This works here because every CDC merge commits an append/overwrite snapshot and
`source_ts_ms`/`ingest_ts` are stored on each row — so "which snapshot did the
enriched row come from?" is a query, not a guess. A model that can't be evaluated
against the data it actually saw is a model you can't defend.

## Verifying it works

```bash
make cdc-status     # connector RUNNING, tasks 0/1 RUNNING, source record count grows
make cdc-demo       # 5 rounds of INSERT/UPDATE/DELETE, printing the exact SQL it runs
make cdc-sync       # apply whatever is in the topic now
make cdc-check      # row counts: Postgres vs the lakehouse  → must be equal
python scripts/check_cdc_parity.py   # same thing, verbose, with fix hints
```

`cdc-demo` mutates the *source of truth* the way an application would: rename a
merchant, close one (the `merchant_closed` flag CDC turns into
`merchant_closed_hit`), change a `txn_limit_1h`, delete a row, insert a new one.
Watch `spark-sql> select merchant_id, merchant_risk_score, op, source_ts_ms from
lake.dim.merchants order by source_ts_ms desc limit 10` while it runs.

## <a name="operating-safely"></a>Operating safely (this is the part that bites)

1. **A dead connector grows your database disk.** A replication slot retains WAL
   until the consumer catches up. Symptom: `pg_replication_slots.restart_lsn` far
   behind, `pg_wal/` exploding, Postgres slow. Check:
   `SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots;`
   Fix forward: restart the connector; if it will never come back,
   `SELECT pg_drop_replication_slot('haweye_cdc_slot');` — **only** when you accept
   losing the un-captured changes (Debezium's `snapshot.mode=initial` will
   re-snapshot on the next start).
2. **Don't rename columns silently.** Debezium will happily emit the new shape;
   our merge reads by name, so a rename = the column stops updating (no error).
   Rename procedure: add the new column, deploy code reading it, backfill, drop the
   old one.
3. **Schema changes need a re-snapshot or an explicit ALTER.** Adding a nullable
   column: add it in Postgres, then `ALTER TABLE lake.dim.merchants ADD COLUMNS
   (new_col string)`. Adding a NOT NULL column with a default: same, but the
   existing Iceberg rows stay NULL — expected, not corruption.
4. **The connector owns its offsets** (Kafka topic `connect_offsets`). Deleting
   that topic while keeping the slot replays the whole retained WAL; deleting the
   slot but keeping the offsets silently starts "wherever Postgres still has WAL".
   If you must reset, drop both together and re-snapshot.
5. **`errors.tolerance=all` + a DLQ topic** (`haweye-dimensions.dlq`) means one bad
   row never blocks replication — but *look at the DLQ*: with tolerance=all,
   failures are invisible by default. `make cdc-status` prints its count.
6. **Snapshot mode on a big table**: `initial` reads the whole table in one
   transaction — for millions of rows, expect load on the primary and long
   transactions; use `snapshot.mode=initial_only`, or a batch backfill of the
   dimension plus `snapshot.mode=no_data` for the deltas.
7. **Never write dimensions into Iceberg by hand.** The next CDC event overwrites
   you, and someone will spend a day on it. Change the business database; that's
   what "source of truth" means.

## Cost check (so you can decide honestly)

One extra Postgres, one extra Kafka, one Connect worker, one slot on the source,
and either a 24/7 Spark job or a 5-minute scheduled one. For two small dimension
tables that is a lot of machinery — which is exactly why the batch variant
(`make cdc-sync` from Airflow) is the default recommendation and the streaming
variant is the opt-in. If your dimensions change twice a day, use the JDBC load in
`backfill_training_data.py` and skip this file.
