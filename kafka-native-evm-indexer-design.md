# Kafka-Native EVM Indexer — Design & Build Plan

## 1. Context & Goals

A Kotlin, Kafka Streams-first EVM event indexer. Replaces Eventeum's architecture
(dynamic contract subscription, reorg-aware confirmation lifecycle) while removing
its dependency on MongoDB and its bespoke instance-coordination protocol. State
lives entirely in Kafka Streams (GlobalKTable / KTable, RocksDB-backed, standby
replicas) rather than an external database. HA and failover come from Kafka
consumer-group rebalancing, not a custom leader-election layer.

Two ingestion sources run in parallel per network — WebSocket subscription
(low latency, occasionally lossy) and REST polling (`eth_getLogs`, higher
latency, deterministic) — reconciled against each other. Confirmed, decoded
events are the deliverable, published to downstream Kafka topics and optionally
materialized into Postgres by a separate dedup consumer.

Not married to Spring Boot. Stack decision (final): plain Kotlin + Kafka
Streams DSL + **Ktor** for the subscription REST API and Interactive Queries
exposure, **Hoplite** for typed YAML configuration (Spring-`@ConfigurationProperties`-like
experience without Spring), and **Micrometer + micrometer-registry-prometheus**
for metrics, exposed via Ktor's official `ktor-server-metrics-micrometer`
plugin. No Spring Boot — Ktor + Hoplite + Micrometer covers all three stated
requirements (YAML config, Prometheus-compatible metrics, plays fine alongside
a `KafkaStreams` instance in the same JVM process) without the extra weight.

Topic serialization: **plain JSON** (via `kotlinx.serialization`), not Avro.
Rationale: the variable part of every message — the ABI-decoded event fields —
isn't a schema you control, it's dictated by whatever contract you happen to
subscribe to. Avro's schema-evolution governance is most valuable when you
control the schema and have multiple independent consumers; neither is true
here for the decoded-fields portion, so Avro's overhead (per-event-type schema
registration, or collapsing to a generic map that throws away its type-safety
benefit anyway) isn't worth it. JSON also keeps local debugging trivial (read
a message in Kafbat UI without a schema-registry round-trip) during active
development. Revisit only if concrete compatibility incidents on the
*envelope* schema (not the decoded-fields blob) start happening — JSON Schema
+ a registry is a viable upgrade path later without switching to Avro.

**Critical numeric rule**: on-chain integers (`uint256`, `int256`, etc.) must
never be serialized as JSON numbers. web3j already decodes these correctly
into `BigInteger` internally (`Uint256`/`Int256`/etc. are `BigInteger`-backed
by design, since Ethereum numeric types are 256-bit and no JVM primitive
fits), so decoding itself is safe. The risk is entirely at the serialization
boundary: always call `.toString()` on `BigInteger` values when building the
`decodedFields` blob, unconditionally, regardless of magnitude. Do not rely on
"small numbers are probably fine as JSON numbers" — pick one rule and apply it
uniformly. Cover this with a unit test using both a tiny value and a
near-2^256 value, in both the encode and any re-decode direction.

---

## 2. High-Level Architecture

```
                        ┌────────────────────────────┐
                        │   Subscription REST API      │
                        │   (Ktor/Javalin)              │
                        │   POST/DELETE /subscriptions  │
                        │   GET  /subscriptions          │
                        └──────────────┬─────────────┘
                                       │ produces
                                       ▼
                        subscriptions-topic (compacted)
                                       │
                                       ▼
                        [GlobalKTable: subscriptions]
                    materialized fully on every instance
                                       │
                    ┌──────────────────┼──────────────────┐
                    ▼                                      ▼
        ┌───────────────────────┐              ┌───────────────────────┐
        │  WS Listener            │              │  REST Poller             │
        │  per network             │              │  per network              │
        │  eth_subscribe(logs)     │              │  eth_getLogs (batched)    │
        │  reads active contracts  │              │  reads active contracts   │
        │  from GlobalKTable       │              │  from GlobalKTable        │
        └───────────┬────────────┘              └────────────┬─────────────┘
                    │ tagged source=ws                        │ tagged source=poll
                    └─────────────────┬────────────────────┘
                                      ▼
                        raw-logs-topic (partitioned by network)
                                      │
                                      ▼
                        [Decode step: ABI decode via web3j]
                                      │
                                      ▼
                        decoded-logs-topic (partitioned by network)
                                      │
                    ┌─────────────────┼─────────────────────┐
                    ▼                                        ▼
        [Reconciliation KTable]                  [Block-tracking KTable]
        key: (network, tx_hash, log_index)         key: network
        value: {seen_ws, seen_poll,                 value: {last_block,
                first_seen_at, decoded_event}                recent_block_hashes[],
                    │                                         reorg_depth_watermark}
                    ▼                                        │
        [Confirmation KTable]  ◄────────────────────────────┘
        key: (network, tx_hash, log_index)
        value: {status: UNCONFIRMED|CONFIRMED|INVALIDATED,
                confirmations_seen, decoded_event}
        driven by a punctuator tied to block-tracking advances
                    │
        ┌───────────┼────────────────┐
        ▼                            ▼
  confirmed-events-topic     reconciliation-anomalies-topic
  (final output, per-event    (ws_gap_suspected, poll_only_confirmed,
   type, e.g. erc20.transfer,  divergent_decode, etc. — for alerting)
   erc20.approval)
                    │
                    ▼
        [Dedup consumer / Postgres sink]
        idempotent upsert on (network, tx_hash, log_index)
        separate process/consumer group — NOT part of the Streams app
```

### Module boundaries

| Module | Responsibility | Owns |
|---|---|---|
| `schema` | Shared JSON schemas (`kotlinx.serialization`) + Kotlin data classes for every topic contract; envelope fields fixed, `decodedFields: JsonObject` deliberately open | Nothing runtime — pure contract definitions, built first |
| `subscription-api` | Ktor REST API for add/remove/list contract subscriptions, Hoplite-driven YAML config, Micrometer/Prometheus metrics endpoint | `subscriptions-topic` producer, GlobalKTable Interactive Query reads |
| `ingestion-ws` | Per-network WebSocket listener(s) | Emits to `raw-logs-topic`, tagged `source=ws`; owns reconnect + gap-poll logic |
| `ingestion-poll` | Per-network REST poller | Emits to `raw-logs-topic`, tagged `source=poll` |
| `decoder` | ABI-based log decoding (web3j) | `raw-logs-topic` → `decoded-logs-topic` |
| `streams-topology` | The core Kafka Streams app: reconciliation KTable, block-tracking KTable, confirmation KTable, punctuator logic | All KTables, all Interactive Query stores |
| `postgres-sink` | Separate consumer (or Kafka Connect JDBC Sink Connector — evaluate before hand-rolling) materializing `confirmed-events-topic` into Postgres | Idempotent upserts, its own consumer group |
| `integration-tests` | End-to-end test harness | Testcontainers stack, Anvil-forked chain fixtures |

Each module is a separate Gradle subproject with its own build file, but all
depend on `schema` for topic contracts — **`schema` must be finalized before
other modules are built in parallel**, or subagents will produce incompatible
message shapes.

---

## 3. Key Design Decisions (already made — do not relitigate without flagging)

1. **No MongoDB, no external state store.** All indexer state is Kafka
   Streams-managed (RocksDB-backed KTables/GlobalKTable). Postgres, if used,
   is a downstream read-model only, populated by a separate consumer — never
   queried or written by the Streams app itself.
2. **HA via consumer group rebalancing**, not custom leader election. Multiple
   instances of the Streams app in one `application.id`; partition assignment
   (by network, or network+contract) is the failover mechanism.
   `num.standby.replicas >= 1` for fast failover.
3. **Dual ingestion (WS + poll) per network**, reconciled via a shared key
   `(network, tx_hash, log_index)`. WS is the fast/unconfirmed path; poll is
   required for `CONFIRMED` status — confirmation must not depend on WS alone.
4. **Dynamic subscriptions via GlobalKTable**, not config file, not restart.
   `subscriptions-topic` is the single source of truth for what's being
   watched; REST API is a thin producer + Interactive Query reader.
5. **Explicit reorg lifecycle**: `UNCONFIRMED → CONFIRMED → INVALIDATED`,
   confirmation threshold configurable per network (different finality
   assumptions per chain — e.g. more confirmations for L1 than an L2 with
   fast finality).
6. **Reconciliation anomalies are a first-class output**, not just logs —
   published to their own topic for alerting/metrics, distinguishing at least:
   `ws_gap_suspected`, `poll_only_confirmed`, `divergent_decode`.
7. **WS reconnect must trigger an explicit backfill poll** for the outage
   window before resuming the subscription — never trust WS to replay missed
   logs.
8. **Build and prove single-source (poll-only) first**, internally, before
   wiring WS — the plan below still parallelizes both since we know the
   target shape, but integration tests must validate poll-only correctness
   before dual-source reconciliation tests are trusted.
9. **Stack**: Ktor (REST + Interactive Query exposure) + Hoplite (typed YAML
   config) + Micrometer/Prometheus (metrics). No Spring Boot.
10. **Serialization**: plain JSON via `kotlinx.serialization`, no Avro, no
    schema registry initially. Every message is an envelope of fixed,
    controlled fields (event name, hashes, block number, status, source)
    plus a `decodedFields: JsonObject` blob whose shape is dictated by
    whatever ABI produced it. All on-chain integer types are serialized as
    strings, unconditionally — never as JSON numbers.
11. **Per-network confirmation depth defaults** (starting values — adjust if
    real-world behavior on a given chain warrants it):
    - Ethereum: **12** blocks. Matches Binance's long-standing production
      threshold for crediting ETH/ERC20 deposits (reduced from 30 in 2019,
      unchanged since) — a reasonable, externally-validated bar for a
      chain with ~12s blocks and ~13min cryptographic finality.
    - Arbitrum, Base, Optimism (OP Stack, single-sequencer L2s): **20**
      blocks. These chains have near-instant sequencer soft-confirmation
      and no competing block producers under normal operation, so deep
      local reorgs are rare, but sequencer restarts/incidents have caused
      shallow reorgs historically — 20 blocks is cheap insurance given
      sub-second-to-2s block times (worst case a few tens of seconds of
      added latency).
    - Polygon PoS: **128** blocks. Polygon has a documented history of
      deeper reorgs than other EVM chains (pre-2022 Bor client issues);
      Heimdall v2 (July 2025) substantially improved finality speed, so
      this default is intentionally conservative rather than
      finely-tuned — revisit if it proves unnecessarily cautious in
      practice, but don't casually shrink it without checking current
      Polygon finality guidance first.
12. **Deep reorgs (beyond the confirmation depth) are explicitly out of
    scope.** No retroactive correction of already-`CONFIRMED` events, and
    no dedicated detection/alerting mechanism for this specific case either
    — deliberately avoiding the added complexity given this is a personal
    project, not a system handling real financial settlement. This is an
    accepted, understood risk: on the confirmation depths above, a deep
    reorg is a rare, consensus-level event, and if one ever silently slips
    through, the cost of being wrong is low. Do not build a
    `reconciliation-anomalies`-style path for this case; the existing
    `UNCONFIRMED → CONFIRMED → INVALIDATED` lifecycle within the
    confirmation window is the full extent of reorg handling required.

---

## 4. Acceptance Criteria

### 4.1 Subscription management
- [ ] `POST /subscriptions` with `{network, address, abi_ref, start_block}`
      results in the contract appearing in every instance's GlobalKTable
      within a bounded time window (define and test the bound, e.g. < 2s
      under test-cluster conditions).
- [ ] `DELETE /subscriptions/{id}` stops both WS and poll ingestion for that
      contract on all instances without requiring a restart.
- [ ] `GET /subscriptions` returns consistent results regardless of which
      instance answers the request (GlobalKTable convergence verified across
      ≥3 instances in a test).
- [ ] Subscribing to a contract with an invalid/malformed ABI reference is
      rejected at the API layer with a clear error, not silently accepted
      and failed later in the pipeline.
- [ ] Restarting the entire cluster preserves all subscriptions (topic is
      the source of truth, not in-memory state).

### 4.2 Ingestion
- [ ] WS listener reconnects automatically on disconnect and issues a
      `eth_getLogs` catch-up call for the exact gap range before resubscribing.
      No log is lost across a simulated WS disconnect (integration test:
      kill the WS connection mid-stream, verify zero gap in confirmed output).
- [ ] REST poller respects configurable batch size / block range per call
      and handles provider rate-limit errors with backoff, without dropping
      the range it was attempting.
- [ ] Both ingestion paths correctly tag every raw log with its source
      (`ws` or `poll`) and network.
- [ ] Adding a new subscription with a `start_block` in the past triggers a
      correct historical backfill via the poller without WS attempting to
      "backfill" (WS is live-only by construction).

### 4.3 Decoding
- [ ] Every event type declared in `include_events` for a subscribed
      contract is correctly decoded against its ABI, including nested
      tuple/struct fields.
- [ ] Malformed or undecodable logs are routed to a dead-letter topic with
      enough context to debug (raw log + contract + attempted ABI), not
      dropped silently and not crashing the app.

### 4.4 Reconciliation
- [ ] An event seen by both WS and poll within the expected timing window
      produces exactly one entry in the reconciliation KTable, correctly
      merged (not two competing entries).
- [ ] An event seen by poll but never by WS (simulated WS gap) is detected
      and flagged on `reconciliation-anomalies-topic` as `ws_gap_suspected`.
- [ ] An event seen by WS but not corroborated by poll after N blocks is
      flagged as `poll_only_confirmed`-adjacent (name TBD by implementer,
      but the case must be observable, not silent).
- [ ] Reconciliation KTable state correctly expires/compacts old entries —
      define and test retention so the store doesn't grow unbounded.

### 4.5 Confirmation lifecycle & reorg handling
- [ ] Events transition `UNCONFIRMED → CONFIRMED` only after the
      network-specific confirmation threshold is met, verified against
      actual block-tracking state, not wall-clock time.
- [ ] A simulated reorg (fork a local chain, produce a log, reorg it out
      before confirmation threshold) results in the event being marked
      `INVALIDATED` and a correction message published downstream — the
      original `confirmed-events-topic` message (if any was optimistically
      emitted) must be followed by an explicit invalidation, never silently
      contradicted.
- [ ] A reorg deep enough to affect an already-`CONFIRMED` event is
      explicitly out of scope (see design decision 12) — no test is required
      to prove correction behavior for this case, and no detection path
      needs to be built for it. If this ever needs revisiting, it means the
      confirmation depths in decision 11 need raising, not that new
      correction machinery needs to be added.
- [ ] Block-tracking KTable correctly maintains recent block hash ancestry
      per network sufficient to detect reorgs up to the configured depth.

### 4.6 HA / failover
- [ ] Killing one Streams app instance mid-processing results in its
      partitions being reassigned to a surviving instance, verified by
      continued event flow with a bounded gap (measure and assert an upper
      bound, don't just assert "eventually recovers").
- [ ] With `num.standby.replicas >= 1`, failover time is measurably faster
      than cold-start-from-changelog — test both configurations and compare.
- [ ] No duplicate `CONFIRMED` events are produced downstream as a direct
      result of a rebalance (rebalance-induced reprocessing must not leak
      into the final output topic as duplicates beyond what the downstream
      dedup consumer already handles).

### 4.7 Downstream Postgres sink
- [ ] Sink consumer upserts on `(network, tx_hash, log_index)` — replaying
      the entire `confirmed-events-topic` from offset 0 into an empty
      Postgres produces the same end state as playing it once (idempotency
      test, not just an assertion).
- [ ] An `INVALIDATED` correction message updates the corresponding Postgres
      row rather than leaving a stale `CONFIRMED` row behind.
- [ ] Evaluate Kafka Connect JDBC Sink Connector as an alternative to a
      hand-rolled consumer before committing to custom code — document the
      decision either way.

### 4.8 Testing infrastructure (this is a priority, not an afterthought)
- [ ] Pure topology logic (KTable joins, punctuator confirmation logic,
      reconciliation merging) is tested with `TopologyTestDriver` — no
      embedded Kafka needed for this tier, fast and deterministic.
- [ ] Full integration tests run against Testcontainers-managed Kafka +
      Postgres + an Anvil (Foundry) instance forking a real chain at a
      pinned block, so tests are deterministic and don't depend on live
      RPC/testnet availability.
- [ ] At least one integration test deliberately produces a reorg on the
      Anvil fork (mine a block, then reset to an earlier state and mine a
      different one) and asserts the full pipeline reacts correctly
      end-to-end (raw log → decode → reconciliation → confirmation →
      invalidation → Postgres correction).
- [ ] At least one integration test kills and restarts a Streams instance
      mid-test and asserts correctness of the post-recovery state, not just
      "it didn't crash."
- [ ] Load/soak test: sustained log volume across all subscribed contracts
      for a meaningful duration (define: e.g. 30 min) with no unbounded
      memory growth in RocksDB state stores and no consumer lag growth.
- [ ] CI runs the full integration suite on every PR, not just unit tests —
      set this up as part of the initial scaffold, not bolted on later.

---

## 5. TDD & Integration Testing Strategy

This section governs *how* every module gets built, not just what gets
tested. Treat it as binding on every subagent in Phase 1, equally weighted
with the acceptance criteria in section 4.

### 5.1 TDD discipline (non-negotiable)

- Red-green-refactor per unit of behavior: write the failing test, make it
  pass, refactor. Tests are committed in the same commit/PR as the code they
  test — never bolted on afterward as a separate cleanup pass.
- No task or acceptance-criterion checkbox in section 4 is marked done
  without a test that was actually run and failing *before* the
  implementation existed, and passing after. Don't take this on faith from
  a subagent's self-report — spot-check by reverting an implementation and
  confirming its test goes red.
- Name tests so they trace back to section 4 directly where possible (e.g.
  `subscribingWithInvalidAbi_isRejectedAtApiLayer` maps to the 4.1 bullet
  about malformed ABI references) — this makes the mapping between "what we
  promised" and "what's proven" inspectable at a glance, which matters a lot
  more in a multi-agent build than a solo one.

### 5.2 Test pyramid — four distinct layers, don't blur them

1. **Unit tests** (pure functions, no Kafka/network/containers, milliseconds):
   decoding logic, the numeric-serialization rule (decision 10), config
   parsing, punctuator arithmetic.
2. **`TopologyTestDriver` tests** (in-memory Kafka Streams pipe, no real
   broker, seconds): KTable joins, reconciliation-merge logic, confirmation
   state transitions. This is where the bulk of `streams-topology`'s
   correctness should be proven — not in Testcontainers. If a bug could be
   caught here, it should never surface first in a full integration test.
3. **Component/contract tests** (one module's boundary against a real
   dependency it owns): `postgres-sink` against a Testcontainers Postgres,
   `ingestion-poll` against a WireMock-stubbed RPC endpoint. Proves the
   module's actual IO code, not logic already covered by layers 1-2.
4. **Full integration / end-to-end tests** (whole stack: Testcontainers
   Kafka + Postgres + an Anvil fork): proves genuine cross-module behavior —
   an event produced on the Anvil fork flows all the way to a correct
   Postgres row. This tier owns section 4.8's reorg and failover scenarios,
   and nothing else needs to be re-proven here that a lower layer already
   covers.

Push every correctness proof as far down this pyramid as it will go. A
reconciliation-merge bug should be caught by a millisecond
`TopologyTestDriver` test, not discovered forty seconds into a full
Testcontainers run — if you find yourself debugging topology logic via a
full integration test, that's a signal the test belongs at a lower layer.

### 5.3 Anvil fork fixture strategy

- **Pin the fork block explicitly** in a checked-in fixtures config — never
  fork "latest" for tests. This is the single biggest source of
  nondeterministic, flaky integration tests in this kind of project, and
  it's completely avoidable.
- Use `anvil_snapshot` / `evm_revert` between tests within one long-lived
  Anvil instance rather than tearing down and re-forking per test — cuts
  per-test time dramatically since the same base state is reused.
- Build a small, reusable fixture library once, in Phase 0's
  `integration-tests` scaffolding, rather than letting each test hand-roll
  its own on-chain setup: a helper that deploys a minimal test ERC20 and
  emits `Transfer`/`Approval` events with known values, including at least
  one deliberately near-2^256 value to exercise the BigInteger-as-string
  rule end-to-end, not just in a unit test.
- **Reorg simulation needs its own reusable helper** (`ReorgFixture` or
  similar): mine block N with tx A, snapshot, then mine an alternate block N
  with tx B instead of A, and assert the pipeline observes exactly the
  right transition (A → `INVALIDATED`, B decoded and progressing normally).
  Build this once — you'll need it for confirmation-lifecycle tests (4.5)
  and again for HA/failover tests where a rebalance happens to coincide
  with a reorg (4.6), and it should behave identically both times.

### 5.4 Handling async correctness without flakiness

- **Never use `Thread.sleep`** or fixed delays to wait for eventual
  consistency — GlobalKTable convergence, consumer lag catching up,
  rebalance completing. Use Awaitility (`await().atMost(...).until { ... }`)
  polling *actual state*, with a generous but bounded timeout. A
  slow-but-correct assertion beats a flaky one every time.
- A test that fails intermittently gets triaged immediately — fix the root
  cause, or explicitly quarantine it (tagged, tracked, with a documented
  reason) — never silently retried into passing and left in the suite. A
  flaky test that nobody investigates is worse than no test, because it
  erodes trust in every other green checkmark in CI.
- Infra flakiness (Testcontainers slow to start, a port conflict) is fine
  to retry at the CI-runner level. Logic flakiness (a genuine race in your
  own code) is never acceptable to paper over with retries — that's a bug,
  not a test problem.

### 5.5 CI structure

- **Every commit/PR**: unit tests + `TopologyTestDriver` tests. Should
  complete in well under a minute total — this is the tight feedback loop
  every subagent iterates against while writing code.
- **Every PR, gating merge**: the full integration suite via Testcontainers
  + Anvil, including the reorg simulation and kill-instance-mid-test
  scenarios from section 4.8. This must be green before merge — not a
  follow-up task, not something deferred to "before the project is done."
- **Nightly / manually triggered**: the load/soak test from 4.8 (sustained
  volume over the defined duration). Not practical to run on every PR —
  track its results over time (memory growth, consumer lag trend) rather
  than treating it as pass/fail, so slow regressions are visible before
  they become acute.

### 5.6 Definition of done

A feature, or a checkbox in section 4, is only done when:

1. A test exists that would genuinely fail without the implementation —
   verified by actually reverting and re-running it, not assumed.
2. The test lives at the lowest pyramid layer that can actually prove the
   behavior (don't reach for Testcontainers when `TopologyTestDriver` would
   prove the same thing in a hundredth of the time).
3. The test runs in CI, not just locally on whichever machine wrote it.

Reject "it worked when I tried it manually" as a basis for marking anything
done — manual verification doesn't survive a rebase, a dependency bump, or
a different subagent touching adjacent code next week.

---

## 7. Open questions — status

All previously open items are now resolved as of this revision:

- Serialization: **JSON**, not Avro (decision 10).
- Stack: **Ktor + Hoplite + Micrometer**, not Spring Boot, not Javalin
  (decision 9).
- Per-network confirmation depths: **set** — Ethereum 12, Arbitrum/Base/
  Optimism 20, Polygon PoS 128 (decision 11).
- Deep reorg scope: **explicitly out of scope**, no detection or correction
  machinery required (decision 12).

Nothing outstanding before Phase 0 begins. If a genuinely new design question
surfaces during the build (schema module in particular — see Phase 0 step 2
in the build prompt), flag it explicitly rather than letting a subagent
silently decide.
