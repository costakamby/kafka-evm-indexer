package indexer.integrationtests.harness

import indexer.schema.DecodedEventEnvelope
import indexer.schema.json.IndexerJson
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Collections
import java.util.Properties
import java.util.UUID

/**
 * A tiny background consumer that tails confirmed-events-topic from the
 * beginning into an in-memory list, so tests can assert directly on the
 * downstream output stream (4.5: "the original confirmed-events-topic message,
 * if any, must be followed by an explicit invalidation") rather than only on the
 * Postgres end-state, and so failures can dump exactly what was emitted.
 */
class KafkaTopicTail(bootstrapServers: String) : AutoCloseable {
    private val received = Collections.synchronizedList(mutableListOf<DecodedEventEnvelope>())
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

    fun start(): KafkaTopicTail {
        consumer.subscribe(listOf(indexer.streamstopology.Topics.CONFIRMED_EVENTS))
        thread = Thread({
            try {
                while (running) {
                    val records = consumer.poll(Duration.ofMillis(300))
                    for (r in records) {
                        received += IndexerJson.instance.decodeFromString(DecodedEventEnvelope.serializer(), r.value())
                    }
                }
            } catch (_: WakeupException) {
            } finally {
                consumer.close()
            }
        }, "confirmed-events-tail").also { it.isDaemon = true; it.start() }
        return this
    }

    /** Snapshot of everything seen for a given (txHash, logIndex), in arrival order. */
    fun eventsFor(txHash: String, logIndex: Long): List<DecodedEventEnvelope> =
        synchronized(received) { received.filter { it.txHash == txHash && it.logIndex == logIndex } }

    fun all(): List<DecodedEventEnvelope> = synchronized(received) { received.toList() }

    override fun close() {
        running = false
        try {
            consumer.wakeup()
        } catch (_: Exception) {
        }
        thread?.join(3_000)
    }
}
