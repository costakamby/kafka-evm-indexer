# ingestion-ws

Per-network WebSocket listener (`eth_subscribe`). Owns reconnect-with-exact-
gap-catchup logic (design decision 7): WS is never trusted to have replayed
anything it missed while disconnected.

## Reconnect + catch-up

On every reconnect after the first, `WsIngestionRunner` backs off, then
issues an `eth_getLogs` call for the *exact* missed range
`[lastSeenBlock+1, currentHead]` before resubscribing. That catch-up call is
tagged `source=WS`, not `source=POLL` — it's the WS listener closing its
own gap, not an independent poll corroboration, and tagging it `POLL` would
misleadingly inflate confidence that an event has genuinely independent
corroboration in the reconciliation KTable.

A single cycle's failure — fetching active subscriptions, the block-number
baseline call, the catch-up call, or the WS stream itself — is caught,
logged, and retried; it never propagates out of `run()`. (This project once
shipped a version where it did: a ~6 second `subscription-api` restart took
the entire process down, not just the affected network. See
`WsIngestionRunnerTest`'s regression test for the exact scenario.)

## Config: two separate RPC URLs per network

`rpcUrl` (HTTPS, used for the catch-up `eth_getLogs` calls) and `wsRpcUrl`
(needs a real `wss://` endpoint — the free public RPCs used elsewhere in
this project are HTTPS-only). The checked-in `wsRpcUrl` defaults are
placeholders; override both via `.env` — see the root README's
Configuration section for `RPC_URL_<NETWORK>` / `WS_RPC_URL_<NETWORK>`.
Until a real `wss://` endpoint is supplied, this module will fail to
connect for that network — expected, not a bug.

## Scope limits (intentional)

- Never reads or acts on a subscription's `startBlock` — WS is live-only by
  construction; historical backfill from a past `startBlock` is
  `ingestion-poll`'s job.
- The active-address set is re-fetched on every (re)connect, not via a live
  mid-stream ticker — a healthy connection is never torn down purely to
  pick up a new subscription sooner.
