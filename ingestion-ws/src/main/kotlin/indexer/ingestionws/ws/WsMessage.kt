package indexer.ingestionws.ws

import indexer.ingestionws.rpc.JsonRpcResponse
import indexer.ingestionws.rpc.RawLogDto
import indexer.schema.json.IndexerJson
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject

/**
 * A parsed WS text frame from the node: either the `eth_subscribe`
 * acknowledgement (has `id`/`result`/`error`, no `method`) or a live
 * `eth_subscription` push notification carrying a log (has `method`).
 */
sealed interface WsMessage {
    data class SubscribeAck(val response: JsonRpcResponse) : WsMessage
    data class Notification(val log: RawLogDto) : WsMessage
}

/**
 * Pure classification of a raw WS text frame - no sockets, string in,
 * sealed data class out. Used identically whether the frame arrived over a
 * real ktor WebSocket session or a fake one in tests. A JSON-RPC push
 * notification always carries a `method` field; a request/response
 * (the `eth_subscribe` ack) never does.
 */
fun classifyWsMessage(text: String): WsMessage {
    val element = Json.parseToJsonElement(text).jsonObject
    return if (element.containsKey("method")) {
        val notification = IndexerJson.instance.decodeFromJsonElement(EthSubscriptionNotification.serializer(), element)
        WsMessage.Notification(notification.params.result)
    } else {
        val response = IndexerJson.instance.decodeFromJsonElement(JsonRpcResponse.serializer(), element)
        WsMessage.SubscribeAck(response)
    }
}

@kotlinx.serialization.Serializable
data class EthSubscriptionNotification(
    val jsonrpc: String = "2.0",
    val method: String,
    val params: EthSubscriptionParams,
)

@kotlinx.serialization.Serializable
data class EthSubscriptionParams(
    val subscription: String,
    val result: RawLogDto,
)
