package indexer.integrationtests.harness

import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord
import indexer.schema.json.IndexerJson
import indexer.streamstopology.Topics
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Collections
import java.util.Properties
import java.util.UUID

/**
 * Tails raw-logs-topic so a test can gate on the ingestion PATH being live -
 * specifically, that ingestion-ws's live eth_subscribe is actually delivering
 * source=WS raw logs before the reorg's replacement (which only WS re-observes)
 * is mined. Gating on this is what makes the reorg test deterministic rather than
 * racing the WS handshake.
 */
class RawLogTail(bootstrapServers: String) : AutoCloseable {
    private val received = Collections.synchronizedList(mutableListOf<RawLogRecord>())
    private val consumer = KafkaConsumer<String, String>(
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "itest-rawtail-${UUID.randomUUID()}")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        },
    )

    @Volatile
    private var running = true
    private var thread: Thread? = null

    fun start(): RawLogTail {
        consumer.subscribe(listOf(Topics.RAW_LOGS))
        thread = Thread({
            try {
                while (running) {
                    val records = consumer.poll(Duration.ofMillis(300))
                    for (r in records) {
                        received += IndexerJson.instance.decodeFromString(RawLogRecord.serializer(), r.value())
                    }
                }
            } catch (_: WakeupException) {
            } finally {
                consumer.close()
            }
        }, "raw-logs-tail").also { it.isDaemon = true; it.start() }
        return this
    }

    /** True once ingestion-ws has delivered at least one source=WS raw log for [contractAddress]. */
    fun hasWsLogFor(contractAddress: String): Boolean = synchronized(received) {
        received.any { it.source == IngestionSource.WS && it.contractAddress.equals(contractAddress, ignoreCase = true) }
    }

    /** Full arrival-order history for [contractAddress] - for diagnosing cross-source ordering. */
    fun historyFor(contractAddress: String): List<RawLogRecord> = synchronized(received) {
        received.filter { it.contractAddress.equals(contractAddress, ignoreCase = true) }
    }

    override fun close() {
        running = false
        try {
            consumer.wakeup()
        } catch (_: Exception) {
        }
        thread?.join(3_000)
    }
}
