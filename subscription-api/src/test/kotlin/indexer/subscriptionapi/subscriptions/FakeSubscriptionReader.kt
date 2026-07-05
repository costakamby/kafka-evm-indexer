package indexer.subscriptionapi.subscriptions

import indexer.schema.SubscriptionRecord

class FakeSubscriptionReader(private val records: MutableMap<String, SubscriptionRecord> = mutableMapOf()) : SubscriptionReader {
    override fun all(): List<SubscriptionRecord> = records.values.toList()

    fun put(record: SubscriptionRecord) {
        records[record.id] = record
    }
}
