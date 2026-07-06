# kafka-evm-indexer

A Kotlin, Kafka Streams-first EVM event indexer. Dynamically subscribe to
contracts across multiple chains, ingest logs via both WebSocket and REST
polling, decode them against their ABI, reconcile the two ingestion paths
against each other, and track each event through an explicit
`UNCONFIRMED → CONFIRMED → INVALIDATED` reorg-aware lifecycle — all state
lives in Kafka Streams (RocksDB-backed, standby-replicated), with an optional
Postgres read-model for querying.

No Spring Boot. Stack is Kotlin + Kafka Streams DSL + [Ktor](https://ktor.io/)
(REST + Interactive Queries) + [Hoplite](https://github.com/sksamuel/hoplite)
(typed YAML config) + [Micrometer](https://micrometer.io/)/Prometheus (metrics).

The full architecture, every design decision and why it was made, and the
acceptance criteria this project is built against all live in
[`kafka-native-evm-indexer-design.md`](./kafka-native-evm-indexer-design.md).
This README is the practical "how do I run this" companion to that document.

## Architecture, in one picture

```
 Subscription REST API (Ktor)
         │ POST/DELETE/GET /subscriptions
         ▼
 subscriptions-topic (compacted)  ──▶  GlobalKTable (every instance holds it all)
         │                                      ▲
         ▼                                      │ reads active contracts
 ┌───────────────┐                    ┌───────────────┐
 │ ingestion-ws  │                    │ ingestion-poll │
 │ eth_subscribe │                    │ eth_getLogs    │
 └───────┬───────┘                    └───────┬───────┘
         │ source=ws                          │ source=poll
         └──────────────────┬──────────────────┘
                             ▼
                    raw-logs-topic
                             │
                             ▼
                 decoder (ABI decode via web3j)
                             │
                             ▼
                   decoded-logs-topic
                             │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                     ▼
 [Reconciliation]    [Block-tracking]      (per-network reorg
  KTable: merges       KTable: tracks       detection via block
  ws+poll sightings    block hash ancestry  hash ancestry)
        │                    │
        └────────┬───────────┘
                  ▼
         [Confirmation KTable]
    UNCONFIRMED → CONFIRMED → INVALIDATED
    (punctuator driven by block-tracking advances)
                  │
      ┌───────────┴────────────┐
      ▼                        ▼
confirmed-events-topic   reconciliation-anomalies-topic
      │
      ▼
 postgres-sink (idempotent upsert read-model)
```

`subscription-api`, `decoder`, and `streams-topology` all run inside **one
JVM process** (`subscription-api`'s `main()`): the Ktor REST server and the
live `KafkaStreams` instance share a process, so any instance can answer any
Interactive Query against its own local `GlobalKTable` copy of the
subscriptions table — no cross-instance forwarding needed.

## Modules

| Module | What it does |
|---|---|
| `schema` | Shared `kotlinx.serialization` data classes for every topic/KTable contract. On-chain integers are always serialized as strings, never JSON numbers — enforced with a unit test at both a trivial value and near-2²⁵⁶. |
| `topic-admin` | Idempotent, checked-in topic provisioning (partition counts, compaction policy) via the Kafka AdminClient — no manual CLI setup. |
| `subscription-api` | Ktor REST API (`/subscriptions`) + the live Kafka Streams topology (decode → reconciliation → block-tracking → confirmation). |
| `decoder` | ABI-based log decoding (web3j), including nested tuple/struct fields. Malformed logs go to a dead-letter topic, never dropped silently. |
| `streams-topology` | The pure, framework-free Kafka Streams topology library `subscription-api` builds its `KafkaStreams` instance from — reconciliation, block-tracking, and confirmation KTables. |
| `ingestion-ws` | Per-network WebSocket listener (`eth_subscribe`). Reconnects with an exact-gap `eth_getLogs` catch-up before resubscribing — WS is never trusted to replay missed logs. |
| `ingestion-poll` | Per-network REST poller (`eth_getLogs`, batched), with historical backfill for new subscriptions and rate-limit backoff. |
| `postgres-sink` | Consumes `confirmed-events-topic`, idempotently upserts into Postgres, and updates rows in place on an `INVALIDATED` correction. |
| `integration-tests` | Full-stack, real-broker tests: a real chain reorg on a forked Anvil instance, and a kill-and-restart HA recovery test. |

## Quick start

Requires JDK 21, Docker, and (for anything beyond a local Anvil fork) an RPC
provider like [Alchemy](https://www.alchemy.com/) — free tier is fine.

```bash
git clone https://github.com/costakamby/kafka-evm-indexer.git
cd kafka-evm-indexer
cp .env.example .env   # see below for what to fill in
```

**1. Start the dev stack** — Kafka (KRaft), Postgres, [Kafbat UI](https://github.com/kafbat/kafka-ui), and Anvil (only needed if you plan to run the integration tests, not for tracking a real chain):

```bash
docker compose up -d kafka postgres kafbat-ui
```

| Service | Address |
|---|---|
| Kafka | `localhost:9092` |
| Postgres | `localhost:15432` (db/user/password: `indexer`) |
| Kafbat UI | http://localhost:18080 |

**2. Provision topics** (once — Kafka has `auto.create.topics.enable=false`, so this must run before anything else):

```bash
./gradlew :topic-admin:run --args="localhost:9092"
```

**3. Start the services**, each in its own terminal:

```bash
./gradlew :subscription-api:run   # REST API + Kafka Streams topology, port 8081
./gradlew :ingestion-poll:run     # polls subscription-api, emits to raw-logs-topic
./gradlew :postgres-sink:run      # runs its Flyway migration, then sinks confirmed-events-topic → Postgres
./gradlew :ingestion-ws:run       # needs a real wss:// endpoint per network - see Configuration below
```

Each `run` task automatically loads `.env` from the repo root into the
process environment, so RPC URL overrides (below) apply without a manual
`export` step.

**4. Subscribe to a contract:**

```bash
curl -X POST localhost:8081/subscriptions -H 'Content-Type: application/json' -d '{
  "network": "ethereum",
  "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
  "abiRef": "erc20",
  "startBlock": 21000000,
  "includeEvents": ["Transfer"]
}'

curl 'localhost:8081/subscriptions?network=ethereum'
curl -X DELETE localhost:8081/subscriptions/{id}
```

Valid `abiRef` values out of the box: `erc20`, `nested-orders` (checked-in
ABI fixtures under `decoder/src/main/resources/abis/`) — add your own JSON
ABI file there and reference it by filename (without `.json`) to subscribe
to other contract types.

From here, raw logs flow `raw-logs-topic → decoded-logs-topic →
confirmed-events-topic`, watchable live in Kafbat UI, with confirmed/
invalidated rows landing in Postgres's `confirmed_events` table.

## Configuration

Every module's `application.yaml` ships with free public RPC defaults
(`chainId`, `rpcUrl`, `confirmationDepth` per network — Ethereum 12,
Arbitrum/Base/Optimism 20, Polygon 128 confirmations). Override the RPC
endpoints per network via `.env` (see `.env.example`) without editing any
checked-in config:

```bash
RPC_URL_ETHEREUM=https://eth-mainnet.g.alchemy.com/v2/your-key      # ingestion-poll + ingestion-ws's own eth_getLogs catch-up
WS_RPC_URL_ETHEREUM=wss://eth-mainnet.g.alchemy.com/v2/your-key     # ingestion-ws's eth_subscribe (needs a real wss:// endpoint - the checked-in public RPCs are HTTPS-only)
```

Fill in whichever networks you're actually subscribing contracts on; any
left unset keep the checked-in default (and `ingestion-ws` will simply fail
to connect for that one network only).

## Testing

```bash
./gradlew test                      # unit + TopologyTestDriver tests, all modules, no Docker needed
./gradlew :integration-tests:test   # full Testcontainers + Anvil suite - needs Docker and .env exported
```

The integration suite proves the acceptance bar end-to-end against a real
chain fork: a deliberate reorg (an event mined, then reorged out before
confirmation, correctly marked `INVALIDATED`) and a kill-and-restart HA
recovery test, not just "it didn't crash." See
[CONTRIBUTING.md](./CONTRIBUTING.md) for the TDD discipline and test pyramid
this project is built against.

## License

[MIT](./LICENSE)
