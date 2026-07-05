package indexer.ingestionws.kafka

import indexer.schema.EventKey
import indexer.schema.RawLogRecord
import indexer.schema.json.IndexerJson
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.Closeable

/** Small, swappable sink for produced raw logs - real impl below, fakeable in tests. */
interface RawLogProducer {
    fun send(record: RawLogRecord)
}

/**
 * Produces to raw-logs-topic, keyed via [EventKey.of] (network, txHash,
 * logIndex) as mandated by the shared reconciliation key (design decision 3).
 */
class KafkaRawLogProducer(
    private val producer: Producer<String, String>,
    private val topic: String = "raw-logs-topic",
) : RawLogProducer, Closeable {

    override fun send(record: RawLogRecord) {
        val key = EventKey.of(record.network, record.txHash, record.logIndex)
        val value = IndexerJson.instance.encodeToString(RawLogRecord.serializer(), record)
        producer.send(ProducerRecord(topic, key, value))
    }

    override fun close() {
        producer.close()
    }
}
