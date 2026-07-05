package indexer.ingestionws.subscriptions

import indexer.schema.SubscriptionRecord
import indexer.schema.json.IndexerJson
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.encodeURLQueryComponent
import kotlinx.serialization.builtins.ListSerializer

/** Real HTTP implementation of [SubscriptionsReader], against subscription-api's REST endpoint. */
class HttpSubscriptionsReader(
    private val client: HttpClient,
    private val baseUrl: String,
) : SubscriptionsReader {

    override suspend fun fetchSubscriptions(
        network: String?,
        status: SubscriptionStatusFilter,
    ): List<SubscriptionRecord> {
        val params = buildList {
            network?.let { add("network=${it.encodeURLQueryComponent()}") }
            add("status=${status.name}")
        }
        val url = "$baseUrl/subscriptions?" + params.joinToString("&")

        val body = client.get(url).bodyAsText()
        return IndexerJson.instance.decodeFromString(ListSerializer(SubscriptionRecord.serializer()), body)
    }
}
