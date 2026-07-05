package indexer.ingestionpoll.subscriptions

import indexer.schema.SubscriptionRecord
import indexer.schema.json.IndexerJson
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.parameter
import io.ktor.client.statement.bodyAsText
import io.ktor.http.isSuccess

/**
 * Ktor-backed [SubscriptionsReader] against the subscription-api's
 * GET /subscriptions?network=..&status=ACTIVE endpoint (design doc's exact
 * shared contract - do not deviate). Uses the schema module's shared
 * [IndexerJson] config so decoding stays consistent with every other module.
 */
class HttpSubscriptionsReader(
    private val httpClient: HttpClient,
    private val baseUrl: String,
) : SubscriptionsReader {

    override suspend fun activeSubscriptions(network: String?): List<SubscriptionRecord> {
        val response =
            httpClient.get("$baseUrl/subscriptions") {
                parameter("network", network)
                parameter("status", "ACTIVE")
            }
        val body = response.bodyAsText()
        check(response.status.isSuccess()) {
            "subscription-api returned ${response.status} fetching active subscriptions: $body"
        }
        return IndexerJson.instance.decodeFromString(body)
    }
}
