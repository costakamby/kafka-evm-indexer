package indexer.ingestionws.rpc

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.containing
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Component test (pyramid layer 3): proves the actual eth_getLogs /
 * eth_blockNumber JSON-RPC IO against a WireMock-stubbed HTTP RPC endpoint -
 * this is the "either web3j or a raw JSON-RPC call via ktor-client" choice
 * from the task brief; raw JSON-RPC via ktor-client was chosen (see report).
 */
class EthRpcHttpClientTest {

    private lateinit var wireMock: WireMockServer
    private lateinit var client: HttpClient
    private lateinit var rpcClient: EthRpcHttpClient

    @BeforeEach
    fun setUp() {
        wireMock = WireMockServer(wireMockConfig().dynamicPort())
        wireMock.start()
        client = HttpClient(CIO)
        rpcClient = EthRpcHttpClient(client, "http://localhost:${wireMock.port()}/rpc")
    }

    @AfterEach
    fun tearDown() {
        client.close()
        wireMock.stop()
    }

    @Test
    fun `ethBlockNumber parses the hex result into a Long`() = runBlocking<Unit> {
        wireMock.stubFor(
            post(urlEqualTo("/rpc"))
                .withRequestBody(containing("\"method\":\"eth_blockNumber\""))
                .willReturn(
                    aResponse().withStatus(200).withHeader("Content-Type", "application/json")
                        .withBody("""{"jsonrpc":"2.0","id":1,"result":"0x64"}"""),
                ),
        )

        rpcClient.ethBlockNumber() shouldBe 100L
    }

    @Test
    fun `ethGetLogs sends the exact fromBlock-toBlock-address range and parses the log array`() = runBlocking<Unit> {
        wireMock.stubFor(
            post(urlEqualTo("/rpc"))
                .withRequestBody(containing("\"method\":\"eth_getLogs\""))
                .withRequestBody(containing("\"fromBlock\":\"0x65\""))
                .withRequestBody(containing("\"toBlock\":\"0x6e\""))
                .withRequestBody(containing("0xabc"))
                .willReturn(
                    aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(
                        """
                        {"jsonrpc":"2.0","id":2,"result":[
                          {"address":"0xabc","blockHash":"0xbh","blockNumber":"0x65","data":"0xd",
                           "logIndex":"0x0","topics":["0xt0"],"transactionHash":"0xth","transactionIndex":"0x0"}
                        ]}
                        """.trimIndent(),
                    ),
                ),
        )

        val logs = rpcClient.ethGetLogs(fromBlock = 101L, toBlock = 110L, addresses = listOf("0xabc"))

        logs.size shouldBe 1
        logs[0].blockNumber shouldBe "0x65"
        logs[0].transactionHash shouldBe "0xth"

        wireMock.verify(postRequestedFor(urlEqualTo("/rpc")))
    }

    @Test
    fun `ethGetLogs with no addresses does not call the RPC endpoint at all`() = runBlocking<Unit> {
        val logs = rpcClient.ethGetLogs(fromBlock = 1L, toBlock = 2L, addresses = emptyList())

        logs shouldBe emptyList()
        wireMock.verify(0, postRequestedFor(urlEqualTo("/rpc")))
    }

    @Test
    fun `a JSON-RPC error response is surfaced as an EthRpcException`() = runBlocking<Unit> {
        wireMock.stubFor(
            post(urlEqualTo("/rpc")).willReturn(
                aResponse().withStatus(200).withHeader("Content-Type", "application/json")
                    .withBody("""{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"rate limited"}}"""),
            ),
        )

        val ex = shouldThrow<EthRpcException> { rpcClient.ethBlockNumber() }
        ex.code shouldBe -32005
    }
}
