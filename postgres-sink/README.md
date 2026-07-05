# postgres-sink

Materializes `confirmed-events-topic` into Postgres as a downstream read
model (design doc decision 1 - Postgres is never written to by the Streams
app itself, only by this module's own consumer group).

## Decision: hand-rolled consumer, not Kafka Connect JDBC Sink Connector

Design doc 4.7 requires evaluating the Kafka Connect JDBC Sink Connector
before committing to custom code. Evaluated and **rejected in favor of a
hand-rolled `KafkaConsumer`**, for this project, for these reasons:

**Infrastructure cost.** No Kafka Connect worker exists in the current
`docker-compose.yml` dev stack. Adding one is a non-trivial new moving part:
a Connect distributed-mode worker (or standalone, which loses fault
tolerance and isn't a real option for anything beyond a demo), its own
internal topics (`connect-configs`, `connect-offsets`, `connect-status`),
its own JVM to run/monitor/upgrade, and the JDBC Sink Connector plugin JAR
to fetch and version-pin. That is a meaningful ongoing maintenance surface
for a personal project, not a one-line docker-compose addition.

**The upsert semantics we need are exactly the part Connect makes awkward.**
The JDBC Sink Connector's `upsert` insert.mode does per-record upsert keyed
on `pk.fields`, which covers the `(network, tx_hash, log_index)` unique
constraint fine in principle. But `decodedFields` is a `JsonObject` with no
fixed schema (deliberately - schema module design), and Connect's JDBC sink
wants a Connect `Schema`/`Struct` per record (via the JSON converter with
`schemas.enable=true`, or a `SMT` to reshape) to know what columns to
write. Since every event type's `decodedFields` shape differs, we would
either (a) flatten it back to a single opaque JSON *string* column via a
custom SMT anyway - at which point Connect is just plumbing a byte payload
through more infrastructure than a 60-line `KafkaConsumer` loop does - or
(b) fight the connector's schema inference for no benefit, since we already
made the deliberate choice (design doc section 1) not to give
`decodedFields` a fixed schema. Connect earns its keep when many
already-schema'd topics need to fan out to many sink types with no custom
code; here there is one topic, one sink, and the interesting column is
schemaless JSON by design - Connect's value proposition doesn't apply.

**Operational simplicity.** A single `KafkaConsumer` + `HikariCP` +
`Flyway` process, started the same way every other module in this repo is
started (`gradle :postgres-sink:run`), with the exact same "no Spring Boot,
no extra framework" posture as the rest of the stack. It is trivially
testable (see below), trivially debuggable (it's a `for` loop), and adds
zero new infrastructure to `docker-compose.yml`.

**When Connect would be worth it**: if this indexer needed to fan the same
topic out to several different sinks (Postgres + Elasticsearch + S3, say),
or needed exactly-once delivery semantics Connect's transactional producer
support gives "for free," revisit this. For one topic into one Postgres
table, the hand-rolled consumer is the pragmatic choice.

## Schema

`src/main/resources/db/migration/V1__create_confirmed_events.sql` (Flyway,
applied automatically by `ConfirmedEventRepository.migrate()` on startup and
in tests):

```sql
CREATE TABLE confirmed_events (
    id                BIGSERIAL PRIMARY KEY,
    network           VARCHAR(64)  NOT NULL,
    tx_hash           VARCHAR(128) NOT NULL,
    log_index         BIGINT       NOT NULL,
    event_name        VARCHAR(255) NOT NULL,
    signature_hash    VARCHAR(128) NOT NULL,
    contract_address  VARCHAR(128) NOT NULL,
    block_number      BIGINT       NOT NULL,
    status            VARCHAR(32)  NOT NULL,
    source            VARCHAR(16)  NOT NULL,
    decoded_fields    JSONB        NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_confirmed_events_network_tx_log UNIQUE (network, tx_hash, log_index)
);

CREATE INDEX idx_confirmed_events_network_block ON confirmed_events (network, block_number);
CREATE INDEX idx_confirmed_events_status ON confirmed_events (status);
CREATE INDEX idx_confirmed_events_decoded_fields ON confirmed_events USING GIN (decoded_fields);
```

The fixed envelope fields are typed columns; `decoded_fields` is JSONB
because its shape is dictated by whatever ABI produced the event (schema
module design, not something this module should normalize away).

## Upsert / idempotency / INVALIDATED handling

`ConfirmedEventUpsert.SQL` is a single `INSERT ... ON CONFLICT
(network, tx_hash, log_index) DO UPDATE SET ...` statement. Every column,
including `status`, is overwritten on conflict, so:

- Replaying the same message twice is a no-op change (idempotent).
- An `INVALIDATED` message for a key that already has a `CONFIRMED` row
  updates that row's `status` in place - it does not insert a second row
  and does not leave the stale `CONFIRMED` value behind.

## Running

`gradle :postgres-sink:run` (config in `src/main/resources/application.yaml`,
defaults match the repo's `docker-compose.yml`: Kafka on `localhost:9092`,
Postgres on `localhost:15432`, db/user/password `indexer`).

## Tests (`gradle :postgres-sink:test`)

| Test class | Pyramid layer (design doc 5.2) | Proves |
|---|---|---|
| `ConfirmedEventUpsertTest` | 1 - unit, no I/O | SQL text has the right `ON CONFLICT` target and column list; `paramsFor` orders values correctly and never emits a raw JSON number for on-chain integers |
| `ConfirmedEventsSinkRunnerTest` | 1 - unit, `MockConsumer` test double, no real broker | The poll loop decodes each record via the shared schema `IndexerJson`, upserts it, and only commits offsets after processing |
| `SinkConfigLoaderTest` | 1 - unit | Hoplite loads `application.yaml` into typed config |
| `ConfirmedEventRepositoryIdempotencyTest` | 3 - component, Testcontainers Postgres | **4.7**: replaying confirmed-events-topic's messages from offset 0 into an empty Postgres produces the same end state (same row count, same column values) as playing it once |
| `ConfirmedEventRepositoryInvalidationTest` | 3 - component, Testcontainers Postgres | **4.7**: an `INVALIDATED` correction updates the existing row in place - no duplicate row, no stale `CONFIRMED` status |

No `Thread.sleep` is used anywhere in this module's tests - the Kafka
consumer tests use a synchronous `MockConsumer` and call `poll()` directly,
so there is no async wait to guard against flakiness.
