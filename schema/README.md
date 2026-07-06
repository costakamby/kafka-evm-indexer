# schema

Shared `kotlinx.serialization` contracts for every Kafka topic and KTable
value shape in this project. No runtime logic — every other module depends
on this one; it depends on nothing in this repo.

## The one hard rule

On-chain integers (`uint256`, `int256`, etc.) must **always** be serialized
as JSON strings, never JSON numbers — many JSON consumers (JS-based tooling
especially) silently lose precision beyond 2⁵³. Every `BigInteger` placed
into a `decodedFields: JsonObject` blob must go through
[`bigIntegerJsonField`](src/main/kotlin/indexer/schema/json/DecodedFieldsJson.kt)
or the [`BigIntegerAsStringSerializer`](src/main/kotlin/indexer/schema/json/BigIntegerAsStringSerializer.kt)
— never a raw `JsonPrimitive(bigIntegerValue)`, which resolves to the
`Number` overload and defeats the whole point.

[`BigIntegerAsStringSerializerTest`](src/test/kotlin/indexer/schema/json/BigIntegerAsStringSerializerTest.kt)
proves this at both a trivial value and 2²⁵⁶−1, in both the encode and
decode direction, and that a raw numeric literal on the wire is rejected
rather than silently accepted.

## Adding a new topic contract

1. Add the data class here, using `IndexerJson` (not a fresh `Json`
   instance) for anything that needs custom serialization.
2. If it carries any on-chain integer, route it through the rule above and
   add a round-trip test proving both the tiny-value and near-2²⁵⁶ case.
3. Any other module wanting to produce/consume the new topic imports the
   class from here — never redefines an equivalent shape locally.

Changing an existing contract touches every module that depends on it by
design — that's the tradeoff for having one source of truth. Flag a
breaking change loudly; don't make it silently.
