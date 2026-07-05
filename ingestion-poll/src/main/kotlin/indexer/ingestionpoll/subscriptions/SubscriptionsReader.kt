package indexer.ingestionpoll.subscriptions

import indexer.schema.SubscriptionRecord

/**
 * Small, swappable boundary onto the subscription-api's REST contract
 * (design doc: GET {baseUrl}/subscriptions?network=..&status=ACTIVE). Kept
 * deliberately narrow - the poller only ever needs ACTIVE subscriptions.
 */
interface SubscriptionsReader {
    suspend fun activeSubscriptions(network: String? = null): List<SubscriptionRecord>
}
