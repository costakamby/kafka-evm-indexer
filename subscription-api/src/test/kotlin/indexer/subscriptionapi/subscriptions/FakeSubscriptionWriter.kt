package indexer.subscriptionapi.subscriptions

import indexer.schema.SubscriptionRecord

class FakeSubscriptionWriter : SubscriptionWriter {
    val published = mutableListOf<SubscriptionRecord>()

    override fun publish(record: SubscriptionRecord) {
        published += record
    }
}
