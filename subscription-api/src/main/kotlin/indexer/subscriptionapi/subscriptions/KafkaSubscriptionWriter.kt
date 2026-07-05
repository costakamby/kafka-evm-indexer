package indexer.subscriptionapi.subscriptions

import indexer.schema.SubscriptionRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

/** Production [SubscriptionWriter]: a plain KafkaProducer to subscriptions-topic, keyed by id. */
class KafkaSubscriptionWriter(
    private val producer: KafkaProducer<String, SubscriptionRecord>,
    private val topic: String,
) : SubscriptionWriter {
    override fun publish(record: SubscriptionRecord) {
        producer.send(ProducerRecord(topic, record.id, record)).get()
    }
}
