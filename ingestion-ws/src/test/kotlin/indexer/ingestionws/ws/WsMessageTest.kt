package indexer.ingestionws.ws

import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test

/**
 * Unit test (pyramid layer 1): pure classification of a raw WS text frame
 * into either the eth_subscribe acknowledgement ({"id":..,"result":"0x.."})
 * or an eth_subscription push notification carrying a log payload. No
 * sockets involved - this is a string-in/data-class-out function.
 */
class WsMessageTest {

    @Test
    fun `classifies a subscribe acknowledgement`() {
        val text = """{"jsonrpc":"2.0","id":1,"result":"0xsubid123"}"""

        val message = classifyWsMessage(text)

        val ack = message.shouldBeInstanceOf<WsMessage.SubscribeAck>()
        ack.response.id shouldBe 1L
        ack.response.error shouldBe null
    }

    @Test
    fun `classifies a subscribe acknowledgement carrying an error`() {
        val text = """{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}"""

        val message = classifyWsMessage(text)

        val ack = message.shouldBeInstanceOf<WsMessage.SubscribeAck>()
        ack.response.error?.code shouldBe -32000
        ack.response.error?.message shouldBe "boom"
    }

    @Test
    fun `classifies an eth_subscription log notification`() {
        val text = """
            {"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xsubid123","result":
              {"address":"0xabc","blockHash":"0xbh","blockNumber":"0x64","data":"0xdata","logIndex":"0x1",
               "topics":["0xt0"],"transactionHash":"0xth","transactionIndex":"0x0"}}}
        """.trimIndent()

        val message = classifyWsMessage(text)

        val notification = message.shouldBeInstanceOf<WsMessage.Notification>()
        notification.log.address shouldBe "0xabc"
        notification.log.blockNumber shouldBe "0x64"
        notification.log.logIndex shouldBe "0x1"
    }
}
