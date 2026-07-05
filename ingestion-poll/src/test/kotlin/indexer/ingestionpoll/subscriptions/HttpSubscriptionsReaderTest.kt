package indexer.ingestionpoll.subscriptions

import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.junit5.WireMockExtension
import indexer.schema.SubscriptionStatus
import io.kotest.matchers.shouldBe
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 * Component/contract test (design doc 5.2 layer 3) against a WireMock stub
 * standing in for the subscription-api module, which doesn't exist yet in
 * this worktree. Locks in the exact shared contract both agents are building
 * against with no live coordination: GET {baseUrl}/subscriptions?network=..
 * &status=ACTIVE -> 200 JSON array of indexer.schema.SubscriptionRecord.
 */
class HttpSubscriptionsReaderTest {

    companion object {
        @JvmField
        @RegisterExtension
        val wireMock: WireMockExtension = WireMockExtension.newInstance().build()
    }

    private fun reader(): HttpSubscriptionsReader =
        HttpSubscriptionsReader(httpClient = HttpClient(CIO), baseUrl = wireMock.baseUrl())

    @Test
    fun `requests status=ACTIVE and the given network, and decodes the SubscriptionRecord list`() {
        wireMock.stubFor(
            get(urlPathEqualTo("/subscriptions"))
                .withQueryParam("network", equalTo("ethereum"))
                .withQueryParam("status", equalTo("ACTIVE"))
                .willReturn(
                    aResponse().withStatus(200).withBody(
                        """
                        [
                          {"id":"sub-1","network":"ethereum","address":"0xabc","abiRef":"erc20-v1","startBlock":100,
                           "includeEvents":["Transfer"],"status":"ACTIVE","createdAtEpochMillis":1000}
                        ]
                        """.trimIndent(),
                    ),
                ),
        )

        val result = runBlocking { reader().activeSubscriptions(network = "ethereum") }

        result shouldBe listOf(
            indexer.schema.SubscriptionRecord(
                id = "sub-1",
                network = "ethereum",
                address = "0xabc",
                abiRef = "erc20-v1",
                startBlock = 100,
                includeEvents = listOf("Transfer"),
                status = SubscriptionStatus.ACTIVE,
                createdAtEpochMillis = 1000,
            ),
        )

        wireMock.verify(
            getRequestedFor(urlPathEqualTo("/subscriptions"))
                .withQueryParam("network", equalTo("ethereum"))
                .withQueryParam("status", equalTo("ACTIVE")),
        )
    }

    @Test
    fun `omits the network query param when polling across all networks`() {
        wireMock.stubFor(
            get(urlPathEqualTo("/subscriptions"))
                .withQueryParam("status", equalTo("ACTIVE"))
                .willReturn(aResponse().withStatus(200).withBody("[]")),
        )

        val result = runBlocking { reader().activeSubscriptions(network = null) }

        result shouldBe emptyList()
        wireMock.verify(
            getRequestedFor(urlPathEqualTo("/subscriptions"))
                .withQueryParam("status", equalTo("ACTIVE"))
                .withoutQueryParam("network"),
        )
    }
}
