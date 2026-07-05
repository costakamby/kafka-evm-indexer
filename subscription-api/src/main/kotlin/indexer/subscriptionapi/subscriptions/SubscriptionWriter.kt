package indexer.subscriptionapi.subscriptions

import indexer.schema.SubscriptionRecord

/** Produces a [SubscriptionRecord] to subscriptions-topic, keyed by its id. */
fun interface SubscriptionWriter {
    fun publish(record: SubscriptionRecord)
}
