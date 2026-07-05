package indexer.schema.json

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import java.math.BigInteger

/**
 * On-chain integers (uint256/int256/etc.) must never be serialized as JSON
 * numbers, regardless of magnitude - many JSON consumers (JS-based tooling
 * in particular) lose precision beyond 2^53. This serializer always renders
 * a BigInteger as a JSON string, and rejects raw numeric literals on decode
 * rather than silently accepting them.
 */
object BigIntegerAsStringSerializer : KSerializer<BigInteger> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("BigInteger", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: BigInteger) {
        val jsonEncoder = encoder as? JsonEncoder
            ?: error("BigIntegerAsStringSerializer can only be used with kotlinx.serialization.json.Json")
        jsonEncoder.encodeJsonElement(JsonPrimitive(value.toString()))
    }

    override fun deserialize(decoder: Decoder): BigInteger {
        val jsonDecoder = decoder as? JsonDecoder
            ?: error("BigIntegerAsStringSerializer can only be used with kotlinx.serialization.json.Json")
        val element = jsonDecoder.decodeJsonElement().jsonPrimitive
        require(element.isString) { "expected on-chain integer to be encoded as a JSON string, got raw literal: $element" }
        return BigInteger(element.content)
    }
}
