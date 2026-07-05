package indexer.ingestionws.ws

import indexer.ingestionws.rpc.EthRpcException
import indexer.ingestionws.rpc.JsonRpcRequest
import indexer.ingestionws.rpc.RawLogDto
import indexer.schema.json.IndexerJson
import io.ktor.client.HttpClient
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.serialization.json.add
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.putJsonArray

/**
 * Real [WsTransport] implementation: opens a ktor-client WebSocket session
 * to [wsUrl] (must be a real `wss://`/`ws://` endpoint - see application.yaml
 * for why the fleet's free public HTTPS RPC URLs cannot be used here), sends
 * `eth_subscribe("logs", {address: [...]})`, and streams `eth_subscription`
 * notifications via [WsTransport.connectAndStream]'s `onLog` callback until
 * the socket closes (returns normally = "disconnected") or the node returns
 * a subscribe error (throws [EthRpcException]).
 */
class KtorWsTransport(
    private val client: HttpClient,
    private val wsUrl: String,
) : WsTransport {

    override suspend fun connectAndStream(addresses: List<String>, onLog: suspend (RawLogDto) -> Unit) {
        client.webSocket(wsUrl) {
            val subscribeRequest = JsonRpcRequest(
                id = 1L,
                method = "eth_subscribe",
                params = buildJsonArray {
                    add("logs")
                    addJsonObject { putJsonArray("address") { addresses.forEach { add(it) } } }
                },
            )
            send(Frame.Text(IndexerJson.instance.encodeToString(JsonRpcRequest.serializer(), subscribeRequest)))

            for (frame in incoming) {
                if (frame !is Frame.Text) continue
                when (val message = classifyWsMessage(frame.readText())) {
                    is WsMessage.SubscribeAck -> {
                        message.response.error?.let { throw EthRpcException(it.code, it.message) }
                    }
                    is WsMessage.Notification -> onLog(message.log)
                }
            }
        }
    }
}
