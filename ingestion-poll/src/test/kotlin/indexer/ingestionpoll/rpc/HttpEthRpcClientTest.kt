package indexer.ingestionpoll.rpc

import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.exactly
import com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.junit5.WireMockExtension
import io.kotest.matchers.shouldBe
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 * Component/contract test (design doc 5.2 layer 3): ingestion-poll's actual
 * IO code against a WireMock-stubbed RPC endpoint, standing in for a real
 * provider since none is available in this worktree. Covers design doc 4.2:
 * "REST poller respects configurable batch size / block range per call and
 * handles provider rate-limit errors with backoff, without dropping the
 * range it was attempting."
 */
class HttpEthRpcClientTest {

    companion object {
        @JvmField
        @RegisterExtension
        val wireMock: WireMockExtension = WireMockExtension.newInstance().build()
    }

    private fun client(maxRetries: Int = 3, initialBackoffMs: Long = 1, maxBackoffMs: Long = 5): HttpEthRpcClient =
        HttpEthRpcClient(
            httpClient = HttpClient(CIO),
            rpcUrl = wireMock.baseUrl(),
            maxRetries = maxRetries,
            initialBackoffMs = initialBackoffMs,
            maxBackoffMs = maxBackoffMs,
        )

    @Test
    fun `blockNumber parses the hex result into a Long`() {
        wireMock.stubFor(
            post(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$[?(@.method == 'eth_blockNumber')]"))
                .willReturn(aResponse().withStatus(200).withBody("""{"jsonrpc":"2.0","id":1,"result":"0x10d4f"}""")),
        )

        val head = runBlocking { client().blockNumber() }

        head shouldBe 0x10d4fL
    }

    @Test
    fun `getLogs sends the exact configured block range and address and maps hex fields`() {
        wireMock.stubFor(
            post(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]"))
                .willReturn(
                    aResponse().withStatus(200).withBody(
                        """
                        {"jsonrpc":"2.0","id":1,"result":[
                          {
                            "address":"0xaaaabbbbccccddddaaaabbbbccccddddaaaabbb",
                            "topics":["0xTOPIC0","0xTOPIC1"],
                            "data":"0xdeadbeef",
                            "blockNumber":"0x64",
                            "blockHash":"0xblockhash",
                            "transactionHash":"0xtxhash",
                            "logIndex":"0x2"
                          }
                        ]}
                        """.trimIndent(),
                    ),
                ),
        )

        val logs = runBlocking { client().getLogs(fromBlock = 100, toBlock = 149, addresses = listOf("0xaaaabbbbccccddddaaaabbbbccccddddaaaabbb")) }

        logs shouldBe listOf(
            RawLog(
                address = "0xaaaabbbbccccddddaaaabbbbccccddddaaaabbb",
                topics = listOf("0xTOPIC0", "0xTOPIC1"),
                data = "0xdeadbeef",
                blockNumber = 100,
                blockHash = "0xblockhash",
                transactionHash = "0xtxhash",
                logIndex = 2,
            ),
        )

        wireMock.verify(
            postRequestedFor(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$.params[0][?(@.fromBlock == '0x64')]"))
                .withRequestBody(matchingJsonPath("$.params[0][?(@.toBlock == '0x95')]")),
        )
    }

    @Test
    fun `getLogs retries on HTTP 429 without dropping or altering the block range, then succeeds`() {
        wireMock.stubFor(
            post(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]"))
                .inScenario("rate-limit-429")
                .whenScenarioStateIs("Started")
                .willReturn(aResponse().withStatus(429))
                .willSetStateTo("retried"),
        )
        wireMock.stubFor(
            post(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]"))
                .inScenario("rate-limit-429")
                .whenScenarioStateIs("retried")
                .willReturn(aResponse().withStatus(200).withBody("""{"jsonrpc":"2.0","id":1,"result":[]}""")),
        )

        val logs = runBlocking { client().getLogs(fromBlock = 500, toBlock = 599, addresses = listOf("0xabc")) }

        logs shouldBe emptyList()
        wireMock.verify(
            exactly(2),
            postRequestedFor(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$.params[0][?(@.fromBlock == '0x1f4')]"))
                .withRequestBody(matchingJsonPath("$.params[0][?(@.toBlock == '0x257')]")),
        )
    }

    @Test
    fun `getLogs retries on a JSON-RPC rate-limit error object returned with HTTP 200, then succeeds`() {
        wireMock.stubFor(
            post(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]"))
                .inScenario("rate-limit-jsonrpc")
                .whenScenarioStateIs("Started")
                .willReturn(
                    aResponse().withStatus(200)
                        .withBody("""{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"rate limit exceeded"}}"""),
                )
                .willSetStateTo("retried"),
        )
        wireMock.stubFor(
            post(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]"))
                .inScenario("rate-limit-jsonrpc")
                .whenScenarioStateIs("retried")
                .willReturn(aResponse().withStatus(200).withBody("""{"jsonrpc":"2.0","id":1,"result":[]}""")),
        )

        val logs = runBlocking { client().getLogs(fromBlock = 1, toBlock = 2, addresses = listOf("0xabc")) }

        logs shouldBe emptyList()
        wireMock.verify(exactly(2), postRequestedFor(urlEqualTo("/")).withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]")))
    }

    @Test
    fun `getLogs gives up after maxRetries and throws RateLimitExceededException naming the exact range`() {
        wireMock.stubFor(
            post(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]"))
                .willReturn(aResponse().withStatus(429)),
        )

        val exception =
            try {
                runBlocking { client(maxRetries = 2).getLogs(fromBlock = 10, toBlock = 20, addresses = listOf("0xabc")) }
                null
            } catch (e: RateLimitExceededException) {
                e
            }

        requireNotNull(exception) { "expected RateLimitExceededException" }
        exception.fromBlock shouldBe 10L
        exception.toBlock shouldBe 20L
        // first attempt + 2 retries = 3 requests total; the range must never be skipped/altered across retries.
        wireMock.verify(exactly(3), postRequestedFor(urlEqualTo("/")).withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]")))
    }

    @Test
    fun `getLogs fails fast on a non-rate-limit JSON-RPC error without retrying`() {
        wireMock.stubFor(
            post(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]"))
                .willReturn(
                    aResponse().withStatus(200)
                        .withBody("""{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}}"""),
                ),
        )

        val exception =
            try {
                runBlocking { client().getLogs(fromBlock = 1, toBlock = 2, addresses = listOf("0xabc")) }
                null
            } catch (e: RpcErrorException) {
                e
            }

        requireNotNull(exception) { "expected RpcErrorException" }
        exception.code shouldBe -32602

        wireMock.verify(exactly(1), postRequestedFor(urlEqualTo("/")).withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]")))
    }
}
