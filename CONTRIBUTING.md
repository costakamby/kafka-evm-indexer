# Contributing

## TDD is mandatory, not aspirational

Every module in this project was built red-green-refactor: write the failing
test, watch it fail for the right reason, then implement. This isn't a style
preference — several real bugs in this codebase (a cross-task Kafka Streams
partitioning bug, a process-crashing exception-handling gap) were only ever
caught because a test was written to prove the fix, not just to document
behavior after the fact. Contributions are expected to follow the same
discipline:

1. Write the test first. Run it. Confirm it fails for the reason you expect
   (not a compile error, not an unrelated fixture problem).
2. Implement the minimum to make it pass.
3. Refactor if needed, keeping the test green.

Tests land in the same commit/PR as the code they prove — never bolted on
afterward as a separate "add tests" pass.

## The test pyramid

Push every correctness proof as far down this pyramid as it will go — a bug
in Kafka Streams topology logic should be caught by a millisecond
`TopologyTestDriver` test, not discovered forty seconds into a full
Testcontainers run.

1. **Unit tests** — pure functions, no Kafka/network/containers, milliseconds.
   Decoding logic, the numeric-serialization rule, config parsing, retry/backoff
   arithmetic.
2. **`TopologyTestDriver` tests** — in-memory Kafka Streams, no real broker,
   seconds. KTable joins, reconciliation-merge logic, confirmation state
   transitions. Most of `streams-topology`'s correctness lives here.
3. **Component/contract tests** — one module's boundary against a real
   dependency it owns: `postgres-sink` against a Testcontainers Postgres,
   `ingestion-poll` against a WireMock-stubbed RPC endpoint.
4. **Full integration tests** — the whole stack: Testcontainers Kafka +
   Postgres + a real Anvil fork. Proves genuine cross-module behavior (a
   reorg on-chain flowing all the way to a corrected Postgres row) that no
   lower layer can. `integration-tests` owns this tier.

If you find yourself debugging topology logic via a full integration test,
that's a signal the test belongs at a lower layer.

### One layer-4 bug worth knowing about

`TopologyTestDriver` simulates a single partition by default. A cross-task
state-visibility bug in `streams-topology` (two Kafka Streams tasks each
holding their own local copy of a state store, with events for the same
network landing on different tasks) was completely invisible at layers 1–3
and only surfaced in the real, multi-partition `integration-tests` reorg
test. If you're touching partitioning, keying, or anything in
`streams-topology`'s `addStateStore`/co-partitioning logic, that layer-4
suite is the one that actually proves it — don't trust a green
`TopologyTestDriver` run alone for changes in that area.

## Running tests locally

```bash
./gradlew test                      # unit + TopologyTestDriver, all modules, no Docker
./gradlew :integration-tests:test   # full suite - needs Docker + .env exported (cp .env.example .env first)
```

## Flakiness policy

- **Infra flakiness** (Testcontainers slow to start, a port conflict) is
  fine to retry.
- **Logic flakiness** (a genuine race in application code) is never
  acceptable to paper over with retries. Find the root cause, or explicitly
  quarantine the test with a documented reason — a flaky test nobody
  investigates erodes trust in every other green checkmark.
- Never use `Thread.sleep` to wait on eventual consistency. Use Awaitility
  (`await().atMost(...).until { ... }`), polling real state.

## Before opening a PR

- `./gradlew test` passes.
- If you touched anything under `streams-topology` or module wiring
  (Kafka topic keys/partitioning, topology assembly), run
  `./gradlew :integration-tests:test` too — see the note above.
- If you're fixing a bug, the test you added should fail on `master`
  before your fix and pass after. If you can, verify this explicitly
  (temporarily revert your fix locally, confirm the test goes red, restore
  it) rather than assuming.
- New topics or partitioning schemes go through `topic-admin`'s
  `TopicDefinitions`, not manual `kafka-topics.sh` commands.

## Design decisions already made

Read [`kafka-native-evm-indexer-design.md`](./kafka-native-evm-indexer-design.md)
before proposing an architectural change — several choices (no Spring Boot,
JSON over Avro, no MongoDB/external state store, HA via consumer-group
rebalancing rather than custom leader election, deep reorgs beyond the
confirmation depth are out of scope) were made deliberately with documented
rationale. If you think one should change, open an issue explaining why
first, rather than a PR that quietly reverses it.
