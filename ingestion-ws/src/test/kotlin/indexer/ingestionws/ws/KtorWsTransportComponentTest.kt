package indexer.ingestionws.ws

import indexer.ingestionws.rpc.EthRpcException
import indexer.ingestionws.rpc.RawLogDto
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets
import io.ktor.server.application.install
import io.ktor.server.cio.CIO as ServerCIO
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets as ServerWebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Component test (pyramid layer 3, design doc 5.2): proves the real
 * [KtorWsTransport]'s actual wire-level IO (frame encoding, subscribe
 * acknowledgement parsing, notification parsing, clean-close-as-disconnect
 * semantics) against a local fake WS server - never a real chain, never
 * WireMock (WireMock has no real WebSocket support). The orchestration logic
 * this transport plugs into (reconnect + exact-range catch-up + tagging) is
 * proven separately, fast and deterministically, in WsIngestionRunnerTest
 * against a fake WsTransport - this test exists purely to prove the actual
 * ktor-client-websockets glue code works end-to-end over a real socket.
 */
class KtorWsTransportComponentTest {

    private var server: EmbeddedServer<*, *>? = null
    private val client = HttpClient(CIO) { install(ClientWebSockets) }

    @AfterEach
    fun tearDown() {
        client.close()
        server?.stop(gracePeriodMillis = 0, timeoutMillis = 0)
    }

    private fun startFakeNode(onSubscribeFrame: suspend io.ktor.server.websocket.DefaultWebSocketServerSession.(String) -> Unit): Int {
        val embedded = embeddedServer(ServerCIO, port = 0) {
            install(ServerWebSockets)
            routing {
                webSocket("/ws") {
                    val subscribeFrame = incoming.receive() as Frame.Text
                    onSubscribeFrame(subscribeFrame.readText())
                }
            }
        }
        server = embedded
        embedded.start(wait = false)
        return runBlocking { embedded.engine.resolvedConnectors().first().port }
    }

    @Test
    fun `streams a live notification then returns normally when the node closes the connection`(): Unit = runBlocking {
        val port = startFakeNode { subscribeText ->
            val requestId = Json.parseToJsonElement(subscribeText).jsonObject.getValue("id").jsonPrimitive.long
            send(Frame.Text("""{"jsonrpc":"2.0","id":$requestId,"result":"0xsubid123"}"""))
            send(
                Frame.Text(
                    """{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xsubid123","result":
                    {"address":"0xabc","blockHash":"0xbh","blockNumber":"0x64","data":"0xd","logIndex":"0x0",
                     "topics":["0xt0"],"transactionHash":"0xth"}}}""".trimIndent(),
                ),
            )
            close()
        }

        val transport = KtorWsTransport(client, "ws://localhost:$port/ws")
        val received = CopyOnWriteArrayList<RawLogDto>()

        transport.connectAndStream(listOf("0xabc")) { log -> received.add(log) }

        received.size shouldBe 1
        received[0].blockNumber shouldBe "0x64"
        received[0].transactionHash shouldBe "0xth"
    }

    @Test
    fun `a subscribe error response is surfaced as an EthRpcException`(): Unit = runBlocking {
        val port = startFakeNode { subscribeText ->
            val requestId = Json.parseToJsonElement(subscribeText).jsonObject.getValue("id").jsonPrimitive.long
            send(Frame.Text("""{"jsonrpc":"2.0","id":$requestId,"error":{"code":-32000,"message":"nope"}}"""))
        }

        val transport = KtorWsTransport(client, "ws://localhost:$port/ws")

        val ex = shouldThrow<EthRpcException> {
            transport.connectAndStream(listOf("0xabc")) { }
        }
        ex.code shouldBe -32000
    }
}
