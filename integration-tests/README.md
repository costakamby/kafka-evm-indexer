# integration-tests

Full-stack, real-broker tests — the top of the test pyramid
([CONTRIBUTING.md](../CONTRIBUTING.md)). Every other module's tests use
`TopologyTestDriver` or a single Testcontainers dependency; this module
wires all of them together in one JVM against Testcontainers-managed Kafka
and Postgres, and a Testcontainers-managed Anvil fork of a real chain.

## Running

```bash
cp .env.example .env   # if you haven't already
export $(cat .env | grep -v '^#' | xargs)
./gradlew :integration-tests:test
```

Needs Docker. `SoakTest` (the lower-priority load/soak sketch from the
design doc's testing-infrastructure section) is tagged `@Tag("soak")` and
excluded from the default run — it's a sketch, not a stable always-green
test; run it explicitly with `-PincludeSoakTests` if you want it.

## What's actually proven here

- **`ReorgEndToEndTest`** — the real acceptance bar for this whole project's
  reorg handling: deploy a real ERC20 on the Anvil fork, subscribe it, mine
  a transaction, snapshot, revert, mine a *different* transaction at the
  same height, and assert the original is marked `INVALIDATED` end-to-end —
  in both the Kafka output and the final Postgres row — while the
  replacement independently reaches `CONFIRMED`.
- **`RecoveryEndToEndTest`** — kills and restarts a live `KafkaStreams`
  instance mid-processing, and asserts pre-kill confirmations survive,
  in-flight events still confirm post-recovery, and nothing duplicates —
  not just "it didn't crash."
- **`PipelineEndToEndTest`** — the happy-path smoke test: a real Anvil event
  flows through the whole pipeline to a `CONFIRMED` Postgres row.

## Harness (`harness/`)

Every other module's production classes (`EmbeddedSubscriptionApi`,
`EmbeddedPoller`, `EmbeddedWsIngestion`, `EmbeddedPostgresSink`) are
composed in-process here rather than launched as subprocesses — this is
what lets the recovery test kill and restart exactly one `KafkaStreams`
instance and Interactive-Query its local state directly. `FullPipeline`
wires all four together; `HarnessSupport` defines the test-only `testnet`
network (confirmation depth 2, so tests don't need to mine dozens of real
blocks) — a test-only config that never touches any module's checked-in
`application.yaml`.

## Fixtures (`fixtures/`)

`AnvilFixture` wraps a Testcontainers-managed Anvil forking a **pinned**
block (never `"latest"` — the single biggest source of flaky on-chain
tests, and completely avoidable). `Erc20Fixture` deploys a minimal test
ERC20 and drives transfer/mint calls, including a near-2²⁵⁶ value to
exercise the BigInteger-as-string rule against a real chain, not just a
unit test. `ReorgFixture` wraps `evm_snapshot`/`evm_revert` for the
mine-alternate-block-at-the-same-height pattern every reorg test needs.
