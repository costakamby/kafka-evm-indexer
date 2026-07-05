package indexer.ingestionpoll.progress

import indexer.schema.json.IndexerJson
import io.kotest.matchers.shouldBe
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test

/**
 * Component test (design doc 5.2 layer 3, exercised via Kafka's own
 * MockProducer contract test double rather than a live broker) proving the
 * durable poll-progress watermark: recordProgress publishes a
 * PollProgressRecord to the compacted poll-progress-topic AND makes the new
 * value immediately visible for read-your-write consistency, addressing the
 * design doc's "no in-memory-only state that doesn't survive a restart" gap.
 */
class KafkaPollProgressStoreTest {

    private fun mockProducer() = MockProducer(true, StringSerializer(), StringSerializer())

    @Test
    fun `lastPolledBlock is null before any progress has been recorded`() {
        val store = KafkaPollProgressStore(producer = mockProducer())

        store.lastPolledBlock("ethereum", "0xabc") shouldBe null
    }

    @Test
    fun `recordProgress publishes a JSON record keyed by network and contract, and updates the read side immediately`() {
        val producer = mockProducer()
        val store = KafkaPollProgressStore(producer = producer, topic = "poll-progress-topic")

        store.recordProgress("ethereum", "0xABC", 12345)

        store.lastPolledBlock("ethereum", "0xABC") shouldBe 12345

        producer.history() shouldBe listOf(producer.history().single())
        val sent = producer.history().single()
        sent.topic() shouldBe "poll-progress-topic"
        sent.key() shouldBe "ethereum:0xabc"
        val decoded = IndexerJson.instance.decodeFromString<PollProgressRecord>(sent.value())
        decoded.network shouldBe "ethereum"
        decoded.contractAddress shouldBe "0xABC"
        decoded.lastPolledBlock shouldBe 12345
    }

    @Test
    fun `a later recordProgress call overwrites the earlier watermark for the same key`() {
        val producer = mockProducer()
        val store = KafkaPollProgressStore(producer = producer)

        store.recordProgress("ethereum", "0xabc", 100)
        store.recordProgress("ethereum", "0xabc", 200)

        store.lastPolledBlock("ethereum", "0xabc") shouldBe 200
        producer.history().size shouldBe 2
    }

    @Test
    fun `restored initial state seeds the read side without requiring a fresh recordProgress`() {
        val store = KafkaPollProgressStore(producer = mockProducer(), initialState = mapOf("ethereum:0xabc" to 999L))

        store.lastPolledBlock("ethereum", "0xabc") shouldBe 999L
    }
}
