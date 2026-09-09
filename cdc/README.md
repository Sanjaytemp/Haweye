# CDC for the dimension tables (optional, recommended)

Short version: **yes, CDC fits this project**, and it fixes a real defect rather
than being decoration. The fraud pipeline joins every transaction against
merchant/card attributes. If those attributes are refreshed by a nightly batch,
then for up to 24 h the stream is scoring against yesterday's merchant — and a
merchant flagged "closed, risk 0.95" at 10:00 is only visible to fraud scoring
tomorrow night.

The setup here: Postgres (logical decoding) → Debezium on Kafka Connect → Kafka
topics → Spark `MERGE INTO` the Iceberg dimension tables → the feature job
picks them up on its next micro-batch.

```bash
make cdc-up          # stack: postgres-cdc, kafka, connect, connector registered
make cdc-stream      # run the merge as a streaming job (2-5s freshness)
# in another terminal:
make cdc-demo        # dimension rows change on purpose; watch them arrive
make cdc-check       # lakehouse == source?  row counts + a spot check
```

Files: `docker-compose.cdc.yml`, `cdc/sql/00_cdc_setup.sql`,
`cdc/connectors/haweye-dimensions.json`, `jobs/cdc_merge_stream.py`,
`jobs/cdc_merge_batch.py`, `jobs/common/cdc.py` — full explanation in
[`docs/05-cdc.md`](../docs/05-cdc.md).
