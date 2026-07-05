package indexer.ingestionws.rpc

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

@Serializable
data class JsonRpcRequest(
    val jsonrpc: String = "2.0",
    val id: Long,
    val method: String,
    val params: JsonElement,
)

@Serializable
data class JsonRpcErrorDto(
    val code: Int,
    val message: String,
)

@Serializable
data class JsonRpcResponse(
    val jsonrpc: String = "2.0",
    val id: Long? = null,
    val result: JsonElement? = null,
    val error: JsonRpcErrorDto? = null,
)

/** Thrown when a JSON-RPC call returns an `error` object instead of a result. */
class EthRpcException(val code: Int, message: String) : RuntimeException("eth JSON-RPC error $code: $message")

/**
 * Wire shape of a single Ethereum log, as returned both by `eth_getLogs` and
 * inside an `eth_subscription` notification's `params.result`. All numeric
 * fields are hex strings per the JSON-RPC spec - see [HexCodec].
 */
@Serializable
data class RawLogDto(
    val address: String,
    val blockHash: String,
    val blockNumber: String,
    val data: String,
    val logIndex: String,
    val removed: Boolean = false,
    val topics: List<String> = emptyList(),
    val transactionHash: String,
    val transactionIndex: String? = null,
)
