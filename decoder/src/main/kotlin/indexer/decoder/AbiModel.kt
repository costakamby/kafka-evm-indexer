package indexer.decoder

/**
 * Internal, self-contained ABI type model. We deliberately do NOT lean on
 * web3j's TypeReference.makeTypeReference for this: it throws
 * UnsupportedOperationException on tuple syntax ("(uint256,address)"), so it
 * cannot express the nested tuple/struct fields that acceptance criterion 4.3
 * requires. Everything here is pure data + pure functions so it is exhaustively
 * unit-testable with no Kafka, no network, milliseconds (test pyramid layer 1).
 */
sealed interface AbiType {
    /** True if the type is variable-length in the ABI head/tail encoding. */
    val dynamic: Boolean

    /** Canonical Solidity type string used to build an event signature. */
    val canonical: String

    /** Size in bytes this type occupies in the encoding head (static types only). */
    val headSizeBytes: Int
}

data class UintT(val bits: Int) : AbiType {
    override val dynamic = false
    override val canonical = "uint$bits"
    override val headSizeBytes = 32
}

data class IntT(val bits: Int) : AbiType {
    override val dynamic = false
    override val canonical = "int$bits"
    override val headSizeBytes = 32
}

data object AddressT : AbiType {
    override val dynamic = false
    override val canonical = "address"
    override val headSizeBytes = 32
}

data object BoolT : AbiType {
    override val dynamic = false
    override val canonical = "bool"
    override val headSizeBytes = 32
}

/** bytes1..bytes32 */
data class FixedBytesT(val size: Int) : AbiType {
    override val dynamic = false
    override val canonical = "bytes$size"
    override val headSizeBytes = 32
}

data object DynamicBytesT : AbiType {
    override val dynamic = true
    override val canonical = "bytes"
    override val headSizeBytes = 32
}

data object StringT : AbiType {
    override val dynamic = true
    override val canonical = "string"
    override val headSizeBytes = 32
}

data class Component(val name: String, val type: AbiType)

data class TupleT(val components: List<Component>) : AbiType {
    override val dynamic = components.any { it.type.dynamic }
    override val canonical = "(" + components.joinToString(",") { it.type.canonical } + ")"
    override val headSizeBytes = components.sumOf { it.type.headSizeBytes }
}

/** [length] == null means dynamic array `T[]`; otherwise fixed `T[length]`. */
data class ArrayT(val elem: AbiType, val length: Int?) : AbiType {
    override val dynamic = length == null || elem.dynamic
    override val canonical = elem.canonical + if (length == null) "[]" else "[$length]"
    override val headSizeBytes = if (dynamic) 32 else elem.headSizeBytes * length!!
}

/** One declared parameter of an event (name + resolved type + indexed flag). */
data class AbiEventParam(val name: String, val type: AbiType, val indexed: Boolean)

/** A parsed event definition ready for decoding. */
data class AbiEvent(
    val name: String,
    val params: List<AbiEventParam>,
    val anonymous: Boolean,
) {
    val canonicalSignature: String = name + "(" + params.joinToString(",") { it.type.canonical } + ")"
}

/** A parsed ABI: its events indexed by their topic0 signature hash. */
data class ParsedAbi(
    val abiRef: String,
    val events: List<AbiEvent>,
    /** topic0 (0x-prefixed keccak of the canonical signature) -> event. */
    val eventsByTopic0: Map<String, AbiEvent>,
)
