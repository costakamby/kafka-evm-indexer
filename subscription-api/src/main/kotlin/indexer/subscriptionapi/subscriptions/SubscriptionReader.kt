package indexer.subscriptionapi.subscriptions

import indexer.schema.SubscriptionRecord

/**
 * Reads subscriptions directly from the local GlobalKTable state store
 * (Interactive Queries) - NOT by re-reading subscriptions-topic per request
 * (acceptance criterion 4.1). Every instance holds the full table, so any
 * instance's implementation of this interface answers consistently.
 */
fun interface SubscriptionReader {
    fun all(): List<SubscriptionRecord>
}
