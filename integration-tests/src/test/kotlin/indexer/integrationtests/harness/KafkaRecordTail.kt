package indexer.integrationtests.harness

import indexer.schema.DecodedEventEnvelope
import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord
import indexer.schema.json.IndexerJson
import indexer.streamstopology.Topics
import kotlinx.serialization.DeserializationStrategy
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Collections
import java.util.Properties
import java.util.UUID

/**
 * A tiny background consumer that tails a topic from the beginning into an
 * in-memory list, so tests can assert directly on a downstream Kafka output
 * stream (rather than only on a final materialized state like a Postgres
 * row) and dump exactly what was emitted on failure. Generic over the
 * record value type - see [confirmedEventsTail]/[rawLogsTail] below for the
 * two topics this project's tests actually tail.
 */
class KafkaRecordTail<T> private constructor(
    bootstrapServers: String,
    private val topic: String,
    private val deserializer: DeserializationStrategy<T>,
) : AutoCloseable {
    private val received = Collections.synchronizedList(mutableListOf<T>())
    private val consumer = KafkaConsumer<String, String>(
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "itest-tail-${UUID.randomUUID()}")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        },
    )

    @Volatile
    private var running = true
    private var thread: Thread? = null

    fun start(): KafkaRecordTail<T> {
        consumer.subscribe(listOf(topic))
        thread = Thread({
            try {
                while (running) {
                    val records = consumer.poll(Duration.ofMillis(300))
                    for (r in records) {
                        received += IndexerJson.instance.decodeFromString(deserializer, r.value())
                    }
                }
            } catch (_: WakeupException) {
            } finally {
                consumer.close()
            }
        }, "$topic-tail").also { it.isDaemon = true; it.start() }
        return this
    }

    /** Snapshot of everything seen so far, in arrival order. */
    fun all(): List<T> = synchronized(received) { received.toList() }

    override fun close() {
        running = false
        try {
            consumer.wakeup()
        } catch (_: Exception) {
        }
        thread?.join(3_000)
    }

    companion object {
        fun confirmedEventsTail(bootstrapServers: String): KafkaRecordTail<DecodedEventEnvelope> =
            KafkaRecordTail(bootstrapServers, Topics.CONFIRMED_EVENTS, DecodedEventEnvelope.serializer())

        /**
         * Tails raw-logs-topic so a test can gate on the ingestion PATH being
         * live - specifically, that ingestion-ws's live eth_subscribe is
         * actually delivering source=WS raw logs before the reorg's
         * replacement (which only WS re-observes) is mined. Gating on this is
         * what makes the reorg test deterministic rather than racing the WS
         * handshake.
         */
        fun rawLogsTail(bootstrapServers: String): KafkaRecordTail<RawLogRecord> =
            KafkaRecordTail(bootstrapServers, Topics.RAW_LOGS, RawLogRecord.serializer())
    }
}

/** Snapshot of everything seen for a given (txHash, logIndex), in arrival order. */
fun KafkaRecordTail<DecodedEventEnvelope>.eventsFor(txHash: String, logIndex: Long): List<DecodedEventEnvelope> =
    all().filter { it.txHash == txHash && it.logIndex == logIndex }

/** True once ingestion-ws has delivered at least one source=WS raw log for [contractAddress]. */
fun KafkaRecordTail<RawLogRecord>.hasWsLogFor(contractAddress: String): Boolean =
    all().any { it.source == IngestionSource.WS && it.contractAddress.equals(contractAddress, ignoreCase = true) }

/** Full arrival-order history for [contractAddress] - for diagnosing cross-source ordering. */
fun KafkaRecordTail<RawLogRecord>.historyFor(contractAddress: String): List<RawLogRecord> =
    all().filter { it.contractAddress.equals(contractAddress, ignoreCase = true) }
