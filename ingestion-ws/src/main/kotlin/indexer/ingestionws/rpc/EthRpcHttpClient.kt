package indexer.ingestionws.rpc

import indexer.schema.json.IndexerJson
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.add
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.util.concurrent.atomic.AtomicLong

/**
 * Real [EthRpcClient] implementation: raw JSON-RPC over HTTP via ktor-client
 * (chosen over adding web3j-core/web3j-abi - see report for rationale). Used
 * for the reconnect catch-up call (`eth_getLogs`) and establishing a block
 * baseline (`eth_blockNumber`); never for eth_subscribe, which is WS-only.
 */
class EthRpcHttpClient(
    private val client: HttpClient,
    private val rpcUrl: String,
) : EthRpcClient {

    private val idCounter = AtomicLong(1)

    override suspend fun ethBlockNumber(): Long {
        val result = call("eth_blockNumber", JsonArray(emptyList()))
        return HexCodec.toLong(result.jsonPrimitive.content)
    }

    override suspend fun ethGetLogs(fromBlock: Long, toBlock: Long, addresses: List<String>): List<RawLogDto> {
        if (addresses.isEmpty()) return emptyList()

        val params = buildJsonArray {
            addJsonObject {
                put("fromBlock", HexCodec.toHex(fromBlock))
                put("toBlock", HexCodec.toHex(toBlock))
                putJsonArray("address") { addresses.forEach { add(it) } }
            }
        }
        val result = call("eth_getLogs", params)
        return IndexerJson.instance.decodeFromJsonElement(ListSerializer(RawLogDto.serializer()), result)
    }

    private suspend fun call(method: String, params: kotlinx.serialization.json.JsonElement): kotlinx.serialization.json.JsonElement {
        val request = JsonRpcRequest(id = idCounter.getAndIncrement(), method = method, params = params)
        val requestBody = IndexerJson.instance.encodeToString(JsonRpcRequest.serializer(), request)

        val httpResponse = client.post(rpcUrl) {
            contentType(ContentType.Application.Json)
            setBody(requestBody)
        }
        val response = IndexerJson.instance.decodeFromString(JsonRpcResponse.serializer(), httpResponse.bodyAsText())

        response.error?.let { throw EthRpcException(it.code, it.message) }
        return response.result ?: throw EthRpcException(-1, "null result for $method")
    }
}
