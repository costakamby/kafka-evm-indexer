package indexer.ingestionpoll.progress

import indexer.schema.json.IndexerJson
import kotlinx.serialization.encodeToString
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.ConcurrentHashMap

/** The topic name must match the entry added to topic-admin's TopicDefinitions.kt. */
const val POLL_PROGRESS_TOPIC = "poll-progress-topic"

/**
 * Durable [PollProgressStore] backed by a small compacted Kafka topic, keyed
 * by "network:contractAddress" (lowercased for a stable key regardless of
 * address casing). On startup, [initialState] should be seeded from
 * [PollProgressRestorer.restore] so a restart resumes from the last durable
 * watermark instead of reprocessing/losing progress.
 *
 * Reads are served from an in-memory cache updated optimistically on every
 * write (read-your-write consistency for this single-writer-per-key use
 * case); the topic itself remains the durable source of truth for restarts.
 */
class KafkaPollProgressStore(
    private val producer: Producer<String, String>,
    private val topic: String = POLL_PROGRESS_TOPIC,
    initialState: Map<String, Long> = emptyMap(),
) : PollProgressStore {

    private val cache = ConcurrentHashMap(initialState)

    override fun lastPolledBlock(network: String, contractAddress: String): Long? =
        cache[cacheKey(network, contractAddress)]

    override fun recordProgress(network: String, contractAddress: String, lastPolledBlock: Long) {
        val key = cacheKey(network, contractAddress)
        val record = PollProgressRecord(
            network = network,
            contractAddress = contractAddress,
            lastPolledBlock = lastPolledBlock,
            updatedAtEpochMillis = System.currentTimeMillis(),
        )
        producer.send(ProducerRecord(topic, key, IndexerJson.instance.encodeToString(record)))
        cache[key] = lastPolledBlock
    }

    companion object {
        fun cacheKey(network: String, contractAddress: String): String = "$network:${contractAddress.lowercase()}"
    }
}
