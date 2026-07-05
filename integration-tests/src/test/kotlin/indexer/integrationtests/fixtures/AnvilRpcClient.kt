package indexer.integrationtests.fixtures

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.atomic.AtomicInteger

/**
 * Raw JSON-RPC client for Anvil-specific methods (evm_snapshot, evm_revert)
 * that aren't part of web3j's standard Ethereum JSON-RPC surface.
 */
class AnvilRpcClient(private val rpcUrl: String) {
    private val httpClient = HttpClient.newHttpClient()
    private val json = Json { ignoreUnknownKeys = true }
    private val nextId = AtomicInteger(1)

    private fun call(method: String, params: JsonArray = JsonArray(emptyList())): JsonElement {
        val requestBody = buildJsonObject {
            put("jsonrpc", "2.0")
            put("id", nextId.getAndIncrement())
            put("method", method)
            put("params", params)
        }
        val request = HttpRequest.newBuilder(URI.create(rpcUrl))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json.encodeToString(JsonObject.serializer(), requestBody)))
            .build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        val parsed = json.parseToJsonElement(response.body()).jsonObject
        parsed["error"]?.let { error("Anvil RPC error calling $method: $it") }
        return parsed.getValue("result")
    }

    /** Snapshots current EVM state, returning a snapshot id usable with [revert]. */
    fun snapshot(): String = call("evm_snapshot").jsonPrimitive.content

    /** Reverts to a snapshot taken by [snapshot], undoing every block mined since. */
    fun revert(snapshotId: String): Boolean = call("evm_revert", JsonArray(listOf(JsonPrimitive(snapshotId)))).jsonPrimitive.boolean
}
