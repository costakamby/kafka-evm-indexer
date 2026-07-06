# subscription-api

Ktor REST API for managing contract subscriptions, plus the live
`KafkaStreams` instance this project's whole decode/reconciliation/
confirmation pipeline runs in. See the root README's architecture diagram
for why these two things share one process.

## `main()` startup order

1. `TopicSetup.ensureTopics(...)` — topics must exist before Streams starts
   (Kafka has `auto.create.topics.enable=false`).
2. Build the topology from `streams-topology`'s `IndexerTopology.build(...)`
   and start a live `KafkaStreams` instance from it.
3. Start the Ktor server, wiring REST handlers to (a) produce to
   `subscriptions-topic` via a plain `KafkaProducer`, and (b) read the
   `GlobalKTable`'s local state store directly for `GET` — never by
   re-reading the topic per request.

## REST contract

```
POST   /subscriptions              {network, address, abiRef, startBlock, includeEvents}
DELETE /subscriptions/{id}
GET    /subscriptions?network=&status=ACTIVE|REMOVED|ALL   (defaults to ACTIVE)
```

`ingestion-ws` and `ingestion-poll` both poll `GET /subscriptions` for their
active contract list — this exact shape is a cross-module contract, not
just this module's internal concern. `abiRef` validation happens at `POST`
time via `decoder`'s `AbiRegistry` — an unknown or malformed ABI is rejected
with 400 before anything is produced to `subscriptions-topic`.

`DELETE` produces a `REMOVED`-status record under the same key (not a
compaction tombstone) — that's what makes the default `GET` (status=ACTIVE)
naturally stop returning it without any instance needing a restart.

## Config

Per-network `confirmationDepth`/`rpcUrl`/`chainId` live in
`application.yaml`, mirrored exactly across `ingestion-ws` and
`ingestion-poll`'s own config (each module owns its own config class
deliberately — importing another module's config would be an inappropriate
cross-module dependency). See the root README's Configuration section for
the `RPC_URL_<NETWORK>` override mechanism.
