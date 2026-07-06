# streams-topology

The pure Kafka Streams topology this project runs — no Ktor, no HTTP, no
embedded server, only `Topology`-building code. `subscription-api` builds a
live `KafkaStreams` instance from `IndexerTopology.build(...)` at startup;
keeping this module framework-free is what lets almost all of its
correctness be proven with `TopologyTestDriver` in milliseconds (see
[CONTRIBUTING.md](../CONTRIBUTING.md)'s test pyramid).

## The three KTables

- **Block-tracking** (`BlockTrackingProcessor`) — per-network block hash
  ancestry, deep enough to detect a reorg up to that network's configured
  confirmation depth.
- **Reconciliation** (`ReconciliationProcessor`) — merges WS and poll
  sightings of the same event into one entry; flags `WS_GAP_SUSPECTED`,
  `POLL_ONLY_CONFIRMED`, or `DIVERGENT_DECODE` anomalies. Entries are
  removed the instant their purpose is served (either side of the merge
  completes, or an anomaly fires) — this is the store's only bound on
  growth, not a TTL.
- **Confirmation** (also `BlockTrackingProcessor`, via a `STREAM_TIME`
  punctuator) — `UNCONFIRMED → CONFIRMED` only once block-tracking reaches
  the configured depth, driven by actual block advances, never wall-clock
  time. A reorg detected before an event confirms marks it `INVALIDATED`
  instead.

## Partitioning: read this before touching topic keys

`BLOCK_TRACKING`/`RECONCILIATION`/`CONFIRMATION` are `addStateStore(...)`
stores — Kafka Streams gives each **task** (each partition of the
co-partitioned sub-topology) its own local copy. A task can never see
another task's slice. This means every record touching these stores for a
given network must land on the exact same task as that network's
block-tracking updates, or the confirmation punctuator silently stops
promoting/invalidating events on other tasks — a real bug this project hit,
only reproducible with more than one partition (invisible to
`TopologyTestDriver`'s single-partition simulation; see
[`NetworkStreamPartitioner`](src/main/kotlin/indexer/streamstopology/lifecycle/NetworkStreamPartitioner.kt)'s
kdoc for the full story).

The fix, and the pattern to follow for anything new that touches these
stores: **`raw-logs-topic` and `decoded-logs-topic` keep default,
EventKey-based partitioning** — that's the external contract every consumer
of those topics relies on. Each internal processor that needs network
co-location does its own private `.selectKey(network).repartition(...)`
into its own internal topic (`raw-logs-by-network`,
`decoded-logs-by-network`) immediately before the processor that needs it,
symmetrically, rather than silently overriding either external topic's own
partitioning for everyone.

## Config

`NetworkTopologyConfig` carries per-network `confirmationDepth` — passed in
by `subscription-api` at topology-build time, not read from this module's
own YAML (this module has none; it's a library, not a runnable service).
