package indexer.decoder

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.web3j.crypto.Hash

/** Thrown when an ABI JSON document cannot be parsed into events. */
class AbiParseException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Parses raw ABI JSON (the exact JSON you'd get from `solc`/Etherscan) into the
 * internal [ParsedAbi] model, computing each event's topic0 signature hash.
 * Pure and dependency-light: only kotlinx.serialization to read JSON and web3j's
 * keccak (Hash.sha3String) for the signature hash.
 */
object AbiParser {
    private val json = Json { ignoreUnknownKeys = true }

    private val arraySuffix = Regex("""^(.*)\[(\d*)]$""")

    fun parse(abiRef: String, abiJson: String): ParsedAbi {
        val root = try {
            json.parseToJsonElement(abiJson)
        } catch (e: Exception) {
            throw AbiParseException("abiRef '$abiRef' is not valid JSON", e)
        }
        val entries = (root as? JsonArray)
            ?: throw AbiParseException("abiRef '$abiRef' ABI must be a JSON array of entries")

        val events = entries.mapNotNull { entry ->
            val obj = entry.jsonObject
            if (obj["type"]?.jsonPrimitive?.content != "event") return@mapNotNull null
            parseEvent(obj)
        }
        if (events.isEmpty()) {
            throw AbiParseException("abiRef '$abiRef' ABI declares no events")
        }

        val byTopic0 = events.associateBy { Hash.sha3String(it.canonicalSignature) }
        return ParsedAbi(abiRef, events, byTopic0)
    }

    private fun parseEvent(obj: JsonObject): AbiEvent {
        val name = obj["name"]?.jsonPrimitive?.content
            ?: throw AbiParseException("event entry is missing a name")
        val anonymous = obj["anonymous"]?.jsonPrimitive?.content?.toBoolean() ?: false
        val inputs = (obj["inputs"] as? JsonArray) ?: JsonArray(emptyList())
        val params = inputs.map { input ->
            val io = input.jsonObject
            val paramName = io["name"]?.jsonPrimitive?.content ?: ""
            val typeStr = io["type"]?.jsonPrimitive?.content
                ?: throw AbiParseException("event '$name' has an input with no type")
            val indexed = io["indexed"]?.jsonPrimitive?.content?.toBoolean() ?: false
            val components = io["components"] as? JsonArray
            AbiEventParam(paramName, parseType(typeStr, components), indexed)
        }
        return AbiEvent(name, params, anonymous)
    }

    /** Resolves a Solidity type string (+ optional tuple components) to an [AbiType]. */
    fun parseType(typeStr: String, components: JsonArray?): AbiType {
        val arrayMatch = arraySuffix.matchEntire(typeStr)
        if (arrayMatch != null) {
            val baseStr = arrayMatch.groupValues[1]
            val lenStr = arrayMatch.groupValues[2]
            val length = if (lenStr.isEmpty()) null else lenStr.toInt()
            return ArrayT(parseType(baseStr, components), length)
        }

        return when {
            typeStr == "address" -> AddressT
            typeStr == "bool" -> BoolT
            typeStr == "string" -> StringT
            typeStr == "bytes" -> DynamicBytesT
            typeStr == "tuple" -> {
                val comps = components
                    ?: throw AbiParseException("tuple type has no components")
                TupleT(
                    comps.map { c ->
                        val co = c.jsonObject
                        val cname = co["name"]?.jsonPrimitive?.content ?: ""
                        val ctype = co["type"]?.jsonPrimitive?.content
                            ?: throw AbiParseException("tuple component has no type")
                        Component(cname, parseType(ctype, co["components"] as? JsonArray))
                    },
                )
            }
            typeStr.startsWith("uint") -> UintT(typeStr.removePrefix("uint").ifEmpty { "256" }.toInt())
            typeStr.startsWith("int") -> IntT(typeStr.removePrefix("int").ifEmpty { "256" }.toInt())
            typeStr.startsWith("bytes") -> FixedBytesT(typeStr.removePrefix("bytes").toInt())
            else -> throw AbiParseException("unsupported ABI type '$typeStr'")
        }
    }
}
