package indexer.ingestionws.subscriptions

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import indexer.schema.SubscriptionStatus
import io.kotest.matchers.shouldBe
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Component test (pyramid layer 3, design doc 5.2): proves this module's
 * actual HTTP client IO against a WireMock-stubbed subscription-api, per the
 * EXACT shared contract in the task brief:
 *   GET {baseUrl}/subscriptions?network={network}&status={status}
 * The real subscription-api server doesn't exist yet in this worktree (a
 * different agent is building it against this same contract), so WireMock is
 * the closest thing to a real dependency this module can test against.
 */
class HttpSubscriptionsReaderTest {

    private lateinit var wireMock: WireMockServer
    private lateinit var client: HttpClient
    private lateinit var reader: SubscriptionsReader

    @BeforeEach
    fun setUp() {
        wireMock = WireMockServer(wireMockConfig().dynamicPort())
        wireMock.start()
        client = HttpClient(CIO)
        reader = HttpSubscriptionsReader(client, "http://localhost:${wireMock.port()}")
    }

    @AfterEach
    fun tearDown() {
        client.close()
        wireMock.stop()
    }

    @Test
    fun `fetches active subscriptions for a network and parses the SubscriptionRecord array`() = runBlocking<Unit> {
        wireMock.stubFor(
            get(urlPathEqualTo("/subscriptions"))
                .withQueryParam("network", equalTo("ethereum"))
                .withQueryParam("status", equalTo("ACTIVE"))
                .willReturn(
                    aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(
                        """
                        [
                          {"id":"sub-1","network":"ethereum","address":"0xabc","abiRef":"erc20-v1",
                           "startBlock":18000000,"includeEvents":["Transfer"],"status":"ACTIVE",
                           "createdAtEpochMillis":1000}
                        ]
                        """.trimIndent(),
                    ),
                ),
        )

        val result = reader.fetchSubscriptions(network = "ethereum")

        result.size shouldBe 1
        result[0].id shouldBe "sub-1"
        result[0].address shouldBe "0xabc"
        result[0].status shouldBe SubscriptionStatus.ACTIVE
    }

    @Test
    fun `defaults to status=ACTIVE and omits network when not provided`() = runBlocking<Unit> {
        wireMock.stubFor(
            get(urlPathEqualTo("/subscriptions"))
                .withQueryParam("status", equalTo("ACTIVE"))
                .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody("[]")),
        )

        val result = reader.fetchSubscriptions()

        result shouldBe emptyList()
        wireMock.verify(
            com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor(urlPathEqualTo("/subscriptions"))
                .withoutQueryParam("network"),
        )
    }

    @Test
    fun `supports explicitly requesting status=ALL`() = runBlocking<Unit> {
        wireMock.stubFor(
            get(urlPathEqualTo("/subscriptions"))
                .withQueryParam("status", equalTo("ALL"))
                .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody("[]")),
        )

        reader.fetchSubscriptions(status = SubscriptionStatusFilter.ALL) shouldBe emptyList()
    }

    @Test
    fun `returns an empty list when the API has no active subscriptions`() = runBlocking<Unit> {
        wireMock.stubFor(
            get(urlPathEqualTo("/subscriptions"))
                .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody("[]")),
        )

        reader.fetchSubscriptions(network = "polygon") shouldBe emptyList()
    }
}
