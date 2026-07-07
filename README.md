# kafka-evm-indexer

[![CI](https://github.com/costakamby/kafka-evm-indexer/actions/workflows/ci.yml/badge.svg)](https://github.com/costakamby/kafka-evm-indexer/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

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

## Prerequisites

- JDK 21
- A Kafka cluster and a Postgres database — bring your own, or use the
  bundled `docker-compose.yml` for a local sandbox (Option B below)
- An RPC provider like [Alchemy](https://www.alchemy.com/) for any network
  beyond a local Anvil fork — free tier is fine

```bash
git clone https://github.com/costakamby/kafka-evm-indexer.git
cd kafka-evm-indexer
```

**Provision topics once**, before starting anything else (Kafka's
`auto.create.topics.enable=false` in the bundled dev stack, and it's good
practice against any cluster — partition counts and compaction policy
should be explicit, not left to broker defaults):

```bash
KAFKA_BOOTSTRAP_SERVERS=my-kafka:9092 ./gradlew :topic-admin:run
# or pass it positionally: ./gradlew :topic-admin:run --args="my-kafka:9092"
```

## Run it — Option A: against your own Kafka + Postgres

Every module is a plain JVM process configured entirely by environment
variables — no checked-in file needs editing. Set what you need (see
[Configuration](#configuration) for the full list) and start each service:

```bash
export KAFKA_BOOTSTRAP_SERVERS=my-kafka:9092
export POSTGRES_JDBC_URL=jdbc:postgresql://my-postgres:5432/indexer
export POSTGRES_USERNAME=myuser
export POSTGRES_PASSWORD=mypassword
export RPC_URL_ETHEREUM=https://eth-mainnet.g.alchemy.com/v2/your-key
export WS_RPC_URL_ETHEREUM=wss://eth-mainnet.g.alchemy.com/v2/your-key

./gradlew :subscription-api:run   # REST API + Kafka Streams topology, port 8081
./gradlew :ingestion-poll:run     # polls subscription-api, emits to raw-logs-topic
./gradlew :postgres-sink:run      # runs its Flyway migration, then sinks confirmed-events-topic → Postgres
./gradlew :ingestion-ws:run       # needs a real wss:// endpoint per network - see Configuration below
```

Or build once and run the installed distributions directly (e.g. in a
container), same environment variables:

```bash
./gradlew installDist
KAFKA_BOOTSTRAP_SERVERS=my-kafka:9092 ./subscription-api/build/install/subscription-api/bin/subscription-api
```

## Run it — Option B: local sandbox via docker-compose

No infrastructure of your own? The bundled stack gives you Kafka (KRaft),
Postgres, [Kafbat UI](https://github.com/kafbat/kafka-ui), and Anvil (only
needed for the integration tests, not for tracking a real chain):

```bash
cp .env.example .env   # RPC URL overrides - see Configuration below
docker compose up -d kafka postgres kafbat-ui
./gradlew :topic-admin:run --args="localhost:9092"
```

| Service | Address |
|---|---|
| Kafka | `localhost:9092` |
| Postgres | `localhost:15432` (db/user/password: `indexer`) |
| Kafbat UI | http://localhost:18080 |

These are already the checked-in defaults in every module's
`application.yaml`, so no environment variables are needed for this path —
just run each service as in Option A, minus the `export` lines. Every
`./gradlew :<module>:run` task automatically loads `.env` from the repo
root into the process environment too, so RPC URL overrides in `.env` apply
without a manual `export` step.

## Try it: subscribe to a contract

Once `subscription-api` and at least one ingestion module are running
(either option above):

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
confirmed-events-topic`. If you're on the docker-compose sandbox, watch it
live in Kafbat UI; either way, confirmed/invalidated rows land in
Postgres's `confirmed_events` table.

## Configuration

Every setting below has a checked-in default matching the docker-compose
sandbox (Option B) — set only what you need for Option A. None require
editing a checked-in file.

| Variable | Default | Used by | Purpose |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | all services | Kafka cluster address — the same convention used by Kafka Connect and Confluent's own Docker images. |
| `POSTGRES_JDBC_URL` | `jdbc:postgresql://localhost:15432/indexer` | `postgres-sink` | Target database. |
| `POSTGRES_USERNAME` | `indexer` | `postgres-sink` | |
| `POSTGRES_PASSWORD` | `indexer` | `postgres-sink` | |
| `SUBSCRIPTION_API_BASE_URL` | `http://localhost:8081` | `ingestion-ws`, `ingestion-poll` | Where to poll for active subscriptions. |
| `RPC_URL_<NETWORK>` | a free public HTTPS endpoint per network | `ingestion-poll`, `ingestion-ws` | Per-network `eth_getLogs` endpoint (`<NETWORK>` is `ETHEREUM`, `ARBITRUM`, `BASE`, `OPTIMISM`, or `POLYGON`). Free-tier providers cap `eth_getLogs` at a small block range — see `ingestion-poll`'s README if you hit this. |
| `WS_RPC_URL_<NETWORK>` | an unset placeholder | `ingestion-ws` | Per-network `eth_subscribe` endpoint — **needs a real `wss://` URL** (e.g. Alchemy/Infura); the free public HTTPS endpoints above don't support WebSockets. `ingestion-ws` simply won't connect for a network left unset. |

A blank value for any of these is treated as unset (falls back to the
default), so an empty `export FOO=` never silently breaks anything.

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
