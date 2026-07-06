# ingestion-poll

Per-network REST poller (`eth_getLogs`, batched by a configurable block
range), with historical backfill for new subscriptions and rate-limit
backoff that never drops the range it was attempting.

## Durable poll progress

A restart must resume, not restart from scratch or replay unbounded
history — so the last-polled-block watermark per `(network, contract)` is
persisted to its own compacted Kafka topic (`poll-progress-topic`,
`KafkaPollProgressStore`/`PollProgressRestorer`), not held only in memory.
This was a genuine gap the original design doc flagged without prescribing
a mechanism; this module's answer was a durable topic, added to
`topic-admin`'s `TopicDefinitions` alongside the rest.

## Rate limits

Every free-tier RPC provider this project has been tested against
(blastapi.io, Alchemy) caps `eth_getLogs` at a 10-block range. The default
`maxBlockRange` in `application.yaml` (2000) assumes a paid tier; lower it
if you're running against a free-tier endpoint, or you'll see
`RpcErrorException`s naming the working range on every poll cycle.
Rate-limit responses (HTTP 429, or a JSON-RPC error body signalling a rate
limit even over HTTP 200) are retried with exponential backoff — the
attempted range is never silently dropped.

## Historical backfill

A brand-new subscription with a `startBlock` in the past triggers a chunked
backfill from `startBlock` to the current head (`BlockRangePlanner`), then
continues incrementally from head forward on subsequent cycles — it never
re-backfills once caught up.
