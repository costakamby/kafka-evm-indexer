# decoder

ABI-based EVM log decoding (hand-rolled, not web3j's — see below), including
nested tuple/struct fields, with every on-chain integer routed through the
schema module's string-serialization rule at every nesting depth.

## Adding a supported contract/ABI

`AbiRegistry` resolves an `abiRef` (as supplied to `POST /subscriptions`) to
a checked-in classpath resource `/abis/<abiRef>.json`. To support a new
contract type:

1. Drop the ABI JSON at `src/main/resources/abis/<name>.json`.
2. Subscribe using `"abiRef": "<name>"`.

An unknown ref or malformed JSON fails `AbiRegistry.isValid(...)` — the
`subscription-api` REST layer rejects the `POST` with 400 before anything is
produced to `subscriptions-topic`, rather than failing later in the
pipeline. See `erc20.json` and `nested-orders.json` for examples, and
`broken.json` for what a deliberately-malformed fixture looks like (used in
tests, not a real ABI).

## Why not web3j's own ABI decoding

web3j is still used for other things in this project (e.g. its `Hash`
utility for computing event signature topics), but its
`TypeReference.makeTypeReference` doesn't support tuple syntax the way this
project needs for nested struct decoding — hence a hand-rolled decoder here
(`AbiDecoder`/`AbiParser`) instead of web3j's `Contract`/event-decoding
layer.

## Malformed logs

A log that doesn't decode (topic count mismatch, unknown event, missing
ABI) is never dropped silently and never crashes the app — it's routed to a
dead-letter topic (`streams-topology`'s `DecodeTopology` wires this up) with
enough context to debug: the raw log, the `abiRef` that was attempted, and
the specific reason.
