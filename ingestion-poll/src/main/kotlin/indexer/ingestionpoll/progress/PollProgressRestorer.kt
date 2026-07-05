package indexer.ingestionpoll.progress

import indexer.schema.json.IndexerJson
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration

/**
 * Rebuilds the in-memory watermark cache for [KafkaPollProgressStore] from
 * the compacted poll-progress-topic on startup, so a restart resumes from
 * the last durable progress instead of reprocessing everything or losing it
 * (design doc: "no in-memory-only state that doesn't survive a restart").
 * Last write wins per key, mirroring log-compaction semantics.
 */
object PollProgressRestorer {

    fun restore(consumer: Consumer<String, String>, partitions: List<TopicPartition>): Map<String, Long> {
        consumer.assign(partitions)
        consumer.seekToBeginning(partitions)
        val endOffsets = consumer.endOffsets(partitions)

        val state = mutableMapOf<String, Long>()
        val remaining = partitions.filterTo(mutableSetOf()) { (endOffsets[it] ?: 0L) > 0L }

        while (remaining.isNotEmpty()) {
            val records = consumer.poll(Duration.ofMillis(200))
            for (record in records) {
                val value = record.value() ?: continue
                val parsed = IndexerJson.instance.decodeFromString<PollProgressRecord>(value)
                state[KafkaPollProgressStore.cacheKey(parsed.network, parsed.contractAddress)] = parsed.lastPolledBlock
            }
            remaining.removeAll { tp -> consumer.position(tp) >= (endOffsets[tp] ?: 0L) }
        }
        return state
    }
}
