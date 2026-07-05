package indexer.subscriptionapi.subscriptions

import indexer.schema.SubscriptionRecord
import indexer.streamstopology.Topics
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes

/**
 * Production [SubscriptionReader]: Interactive Queries against this instance's
 * own local copy of the subscriptions GlobalKTable store - never re-reads
 * subscriptions-topic per request (acceptance criterion 4.1). Every instance
 * holds the FULL table (GlobalKTable), so this answers consistently regardless
 * of which instance receives the request.
 */
class KafkaStreamsSubscriptionReader(private val streams: KafkaStreams) : SubscriptionReader {
    override fun all(): List<SubscriptionRecord> {
        val store = streams.store(
            StoreQueryParameters.fromNameAndType(Topics.SUBSCRIPTIONS_STORE, QueryableStoreTypes.keyValueStore<String, SubscriptionRecord>()),
        )
        return store.all().use { it.asSequence().map { kv -> kv.value }.toList() }
    }
}
