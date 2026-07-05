package indexer.decoder

import indexer.schema.json.bigIntegerJsonField
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.math.BigInteger

/**
 * A self-contained decoder for the Solidity ABI head/tail encoding. Produces
 * kotlinx.serialization JsonElements, applying the BigInteger-as-string rule
 * (design decision 10) recursively to EVERY numeric leaf, however deeply nested
 * inside tuples/arrays (acceptance criterion 4.3). All on-chain integers go
 * through [bigIntegerJsonField] — never a raw JsonPrimitive(BigInteger).
 */
object AbiDecoder {

    /** Decodes the non-indexed `data` blob (a tuple of the given params) into a name->value map. */
    fun decodeData(data: ByteArray, params: List<Component>): Map<String, JsonElement> {
        val values = decodeTuple(data, 0, params.map { it.type })
        return params.mapIndexed { i, c -> c.name to values[i] }.toMap()
    }

    /**
     * Decodes one indexed event parameter carried in a 32-byte topic. For value
     * types the topic holds the value directly; for dynamic types (string, bytes,
     * arrays, structs) the topic holds only keccak(value) and the value itself is
     * NOT recoverable — we surface the 32-byte hash as a hex string so nothing is
     * silently dropped.
     */
    fun decodeIndexedTopic(topicHex: String, type: AbiType): JsonElement {
        val word = hexToBytes(topicHex)
        require(word.size == 32) { "indexed topic must be 32 bytes, got ${word.size}" }
        return if (isReferenceType(type)) {
            JsonPrimitive("0x" + toHex(word))
        } else {
            decodeValue(word, 0, type)
        }
    }

    private fun isReferenceType(type: AbiType): Boolean =
        type.dynamic || type is TupleT || type is ArrayT

    /**
     * Decodes a sequence of types laid out as an ABI tuple starting at [base].
     * Dynamic members store a 32-byte offset (relative to [base]) in the head and
     * their payload in the tail; static members are inlined in the head.
     */
    private fun decodeTuple(data: ByteArray, base: Int, types: List<AbiType>): List<JsonElement> {
        val results = ArrayList<JsonElement>(types.size)
        var head = base
        for (type in types) {
            if (type.dynamic) {
                val offset = readUint(data, head).toInt()
                results.add(decodeValue(data, base + offset, type))
                head += 32
            } else {
                results.add(decodeValue(data, head, type))
                head += type.headSizeBytes
            }
        }
        return results
    }

    private fun decodeValue(data: ByteArray, pos: Int, type: AbiType): JsonElement = when (type) {
        is UintT -> bigIntegerJsonField(readUint(data, pos))
        is IntT -> bigIntegerJsonField(readInt(data, pos))
        is AddressT -> JsonPrimitive("0x" + toHex(data.copyOfRange(pos + 12, pos + 32)))
        is BoolT -> JsonPrimitive(readUint(data, pos).signum() != 0)
        is FixedBytesT -> JsonPrimitive("0x" + toHex(data.copyOfRange(pos, pos + type.size)))
        is DynamicBytesT -> {
            val len = readUint(data, pos).toInt()
            JsonPrimitive("0x" + toHex(data.copyOfRange(pos + 32, pos + 32 + len)))
        }
        is StringT -> {
            val len = readUint(data, pos).toInt()
            JsonPrimitive(String(data.copyOfRange(pos + 32, pos + 32 + len), Charsets.UTF_8))
        }
        is TupleT -> JsonObject(
            type.components
                .let { comps -> comps.zip(decodeTuple(data, pos, comps.map { it.type })) }
                .associate { (comp, value) -> comp.name to value },
        )
        is ArrayT -> {
            if (type.length == null) {
                val len = readUint(data, pos).toInt()
                val elemsBase = pos + 32
                JsonArray(decodeTuple(data, elemsBase, List(len) { type.elem }))
            } else {
                JsonArray(decodeTuple(data, pos, List(type.length) { type.elem }))
            }
        }
    }

    /** Unsigned big-endian 32-byte word at [pos]. */
    private fun readUint(data: ByteArray, pos: Int): BigInteger =
        BigInteger(1, data.copyOfRange(pos, pos + 32))

    /** Signed (two's complement) big-endian 32-byte word at [pos]. */
    private fun readInt(data: ByteArray, pos: Int): BigInteger =
        BigInteger(data.copyOfRange(pos, pos + 32))

    fun hexToBytes(hex: String): ByteArray {
        val clean = hex.removePrefix("0x")
        require(clean.length % 2 == 0) { "hex string must have even length: $hex" }
        return ByteArray(clean.length / 2) { i ->
            clean.substring(i * 2, i * 2 + 2).toInt(16).toByte()
        }
    }

    private fun toHex(bytes: ByteArray): String =
        bytes.joinToString("") { "%02x".format(it) }
}
