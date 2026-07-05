package indexer.ingestionpoll.rpc

import indexer.schema.json.IndexerJson
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.delay
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.slf4j.LoggerFactory

/** Raised internally when a request should be retried; never escapes [HttpEthRpcClient]. */
private class RateLimitedSignal(message: String) : Exception(message)

/**
 * JSON-RPC client for eth_blockNumber / eth_getLogs, hand-rolled on top of
 * Ktor rather than web3j's own HTTP transport so provider rate-limit
 * responses (HTTP 429, or a JSON-RPC error object indicating rate limiting)
 * can be detected and retried with backoff without ever skipping the block
 * range being attempted (design doc 4.2).
 */
class HttpEthRpcClient(
    private val httpClient: HttpClient,
    private val rpcUrl: String,
    private val maxRetries: Int,
    private val initialBackoffMs: Long,
    private val maxBackoffMs: Long,
) : EthRpcClient {

    private val log = LoggerFactory.getLogger(HttpEthRpcClient::class.java)

    override suspend fun blockNumber(): Long {
        val result = call("eth_blockNumber", buildJsonArray {})
        return hexToLong(result.jsonPrimitive.content)
    }

    override suspend fun getLogs(fromBlock: Long, toBlock: Long, addresses: List<String>): List<RawLog> {
        val params = buildJsonArray {
            add(
                buildJsonObject {
                    put("fromBlock", longToHex(fromBlock))
                    put("toBlock", longToHex(toBlock))
                    put(
                        "address",
                        buildJsonArray { addresses.forEach { add(JsonPrimitive(it)) } },
                    )
                },
            )
        }

        val result =
            try {
                call("eth_getLogs", params)
            } catch (e: RateLimitedSignal) {
                throw RateLimitExceededException(fromBlock, toBlock, e)
            }
        return result.jsonArray.map { it.jsonObject.toRawLog() }
    }

    private suspend fun call(method: String, params: JsonElement): JsonElement {
        var attempt = 0
        while (true) {
            val response: HttpResponse =
                httpClient.post(rpcUrl) {
                    contentType(ContentType.Application.Json)
                    setBody(
                        buildJsonObject {
                            put("jsonrpc", "2.0")
                            put("id", 1)
                            put("method", method)
                            put("params", params)
                        }.toString(),
                    )
                }

            if (response.status.value == 429) {
                attempt = retryOrThrow(attempt, method, "HTTP 429 Too Many Requests")
                continue
            }

            val bodyText = response.bodyAsText()
            val json = IndexerJson.instance.parseToJsonElement(bodyText).jsonObject
            val error = json["error"]?.jsonObject
            if (error != null) {
                if (isRateLimitError(error)) {
                    attempt = retryOrThrow(attempt, method, error.toString())
                    continue
                }
                val code = error["code"]?.jsonPrimitive?.intOrNull
                val message = error["message"]?.jsonPrimitive?.contentOrNull ?: "unknown RPC error"
                throw RpcErrorException("RPC error calling $method: $message", code)
            }

            return json["result"] ?: throw RpcErrorException("missing result field calling $method: $bodyText")
        }
    }

    /** Sleeps for the next backoff interval and returns the incremented attempt count, or throws if exhausted. */
    private suspend fun retryOrThrow(attempt: Int, method: String, reason: String): Int {
        if (attempt >= maxRetries) {
            throw RateLimitedSignal("rate limited calling $method after $attempt retries: $reason")
        }
        val delayMs = Backoff.delayMillis(attempt, initialBackoffMs, maxBackoffMs)
        log.warn("rate limited calling {} (attempt {}/{}), backing off {}ms: {}", method, attempt + 1, maxRetries, delayMs, reason)
        delay(delayMs)
        return attempt + 1
    }

    private fun isRateLimitError(error: JsonObject): Boolean {
        val code = error["code"]?.jsonPrimitive?.intOrNull
        val message = error["message"]?.jsonPrimitive?.contentOrNull.orEmpty().lowercase()
        return code == -32005 || "rate limit" in message || "too many request" in message
    }

    private fun JsonObject.toRawLog(): RawLog =
        RawLog(
            address = this.getValue("address").jsonPrimitive.content,
            topics = (this["topics"] as? JsonArray)?.map { it.jsonPrimitive.content } ?: emptyList(),
            data = this.getValue("data").jsonPrimitive.content,
            blockNumber = hexToLong(this.getValue("blockNumber").jsonPrimitive.content),
            blockHash = this.getValue("blockHash").jsonPrimitive.content,
            transactionHash = this.getValue("transactionHash").jsonPrimitive.content,
            logIndex = hexToLong(this.getValue("logIndex").jsonPrimitive.content),
        )

    companion object {
        fun hexToLong(hex: String): Long {
            val stripped = hex.removePrefix("0x").removePrefix("0X")
            return if (stripped.isEmpty()) 0L else java.lang.Long.parseLong(stripped, 16)
        }

        fun longToHex(value: Long): String = "0x" + value.toString(16)
    }
}
