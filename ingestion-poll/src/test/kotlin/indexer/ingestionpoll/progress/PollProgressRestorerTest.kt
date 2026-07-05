package indexer.ingestionpoll.progress

import indexer.schema.json.IndexerJson
import io.kotest.matchers.shouldBe
import kotlinx.serialization.encodeToString
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test

/**
 * Component test (design doc 5.2 layer 3, via Kafka's own MockConsumer
 * contract test double - no live broker needed) proving restart-safe
 * restore of the poll-progress-topic: last-write-wins per (network,
 * contract) key, exactly like a compacted-topic changelog restore.
 */
class PollProgressRestorerTest {

    private val topic = "poll-progress-topic"
    private val tp = TopicPartition(topic, 0)

    private fun record(network: String, contractAddress: String, lastPolledBlock: Long, offset: Long) =
        org.apache.kafka.clients.consumer.ConsumerRecord(
            topic,
            0,
            offset,
            KafkaPollProgressStore.cacheKey(network, contractAddress),
            IndexerJson.instance.encodeToString(
                PollProgressRecord(network, contractAddress, lastPolledBlock, 1_000L + offset),
            ),
        )

    @Test
    fun `restore returns an empty map for a brand-new, empty topic`() {
        val consumer = MockConsumer<String, String>(OffsetResetStrategy.EARLIEST)
        consumer.updateBeginningOffsets(mapOf(tp to 0L))
        consumer.updateEndOffsets(mapOf(tp to 0L))

        val state = PollProgressRestorer.restore(consumer, listOf(tp))

        state shouldBe emptyMap()
    }

    @Test
    fun `restore keeps only the latest record per key, last write wins`() {
        val consumer = MockConsumer<String, String>(OffsetResetStrategy.EARLIEST)
        consumer.updateBeginningOffsets(mapOf(tp to 0L))
        consumer.updateEndOffsets(mapOf(tp to 3L))
        consumer.schedulePollTask {
            consumer.addRecord(record("ethereum", "0xabc", 100, 0))
            consumer.addRecord(record("ethereum", "0xabc", 200, 1))
            consumer.addRecord(record("polygon", "0xdef", 50, 2))
        }

        val state = PollProgressRestorer.restore(consumer, listOf(tp))

        state shouldBe mapOf(
            "ethereum:0xabc" to 200L,
            "polygon:0xdef" to 50L,
        )
    }
}
