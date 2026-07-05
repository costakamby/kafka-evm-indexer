package indexer.schema.json

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import java.math.BigInteger

/**
 * The only sanctioned way to place an on-chain integer into a decodedFields
 * JsonObject: always a JSON string, never JsonPrimitive(value) - the latter
 * resolves to the Number overload for BigInteger and silently emits a raw
 * numeric literal, which is exactly the bug decision 10 forbids.
 */
fun bigIntegerJsonField(value: BigInteger): JsonElement = JsonPrimitive(value.toString())
