# NiFi (optional ingestion path)

The brief specifies NiFi as the "external source" simulator.  This repo keeps it
**optional but real**, and uses `generator/` as the default, for one reason:
NiFi needs a browser and ~6 minutes of clicking, and nobody learns Spark/Iceberg
by spending their first evening in a processor palette.

| path | when to use |
|---|---|
| `make gen-stream` | default. Same payload, reproducible, no clicking. |
| `make nifi-up` (this folder) | when you want the actual NiFi graph on screen |

## The graph you are building

```
GenerateFlowFile ──► ExecuteScript (Groovy) ──► PublishKafka
   1 file/sec          nifi/flow/GenerateTransactions.groovy   topic: raw_transactions
                                                            kafka:29092
```

1. `make nifi-up` then open <http://localhost:8090/nifi>
2. Drag **GenerateFlowFile** onto the canvas → properties:
   `Batch Size = 1`, `File Size = 0 B`, `Auto-terminated = false`,
   scheduling `1 sec`.
3. Drag **ExecuteScript** → properties: `Script Engine = groovy`, and paste the
   contents of [`flow/GenerateTransactions.groovy`](flow/GenerateTransactions.groovy)
   into *Script Body*.
4. Drag **PublishKafka** (v2_0) → `Kafka Boootstrap Servers = kafka:29092`,
   `Topic Name = raw_transactions`, `Acks = 1`, `Compression Type = lz4`,
   `Key Recording Policy = Kafka Message Key`, `Key Attribute = kafka.key`.
5. Connect them (Generate → Execute → Publish), *Start* all three (play buttons).
6. Verify: `make kafka-tail` — you should see JSON lines flying by.

## Two gotchas that eat beginners

* **NiFi runs inside the compose network.** `localhost:9092` from the host is
  *not* reachable as `localhost` from NiFi; use `kafka:29092`.
* **NiFi has no `python`**: the Groovy script uses only the JDK API, so it works
  in the stock image without installing anything.

## What NiFi is *for* (why teams pick it)

NiFi earns its keep when the *real* source is a bank API, an SFTP drop, an
outbound webhook, a database export - i.e. when you need retries, rate limiting,
encryption, provenance and "no code" changes.  Here it is generating fake data,
so its value is illustrative only; the streaming core (Kafka → Spark →
Iceberg) is identical either way.
