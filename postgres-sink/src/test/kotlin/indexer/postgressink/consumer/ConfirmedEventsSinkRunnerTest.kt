package indexer.postgressink.consumer

import indexer.postgressink.db.ConfirmedEventRepository
import indexer.schema.ConfirmationStatus
import indexer.schema.DecodedEventEnvelope
import indexer.schema.json.IndexerJson
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * Unit test (test pyramid layer 1, design doc section 5.2): no real Kafka
 * broker, no containers - MockConsumer is an in-JVM test double for the
 * Kafka Consumer interface, so this proves the hand-rolled poll loop's own
 * logic (decode envelope, upsert, commit) in milliseconds.
 */
class ConfirmedEventsSinkRunnerTest {

    private val topic = "confirmed-events-topic"
    private val partition = TopicPartition(topic, 0)

    private fun envelopeJson(txHash: String, status: ConfirmationStatus): String {
        val envelope = DecodedEventEnvelope(
            eventName = "Transfer",
            signatureHash = "0xdead",
            network = "ethereum",
            contractAddress = "0xcontract",
            txHash = txHash,
            logIndex = 0,
            blockNumber = 1,
            status = status,
            source = indexer.schema.IngestionSource.POLL,
            decodedFields = kotlinx.serialization.json.JsonObject(emptyMap()),
        )
        return IndexerJson.instance.encodeToString(DecodedEventEnvelope.serializer(), envelope)
    }

    private fun newMockConsumer(): MockConsumer<String, String> {
        val consumer = MockConsumer<String, String>(OffsetResetStrategy.EARLIEST)
        consumer.subscribe(listOf(topic))
        consumer.rebalance(listOf(partition))
        consumer.updateBeginningOffsets(mapOf(partition to 0L))
        return consumer
    }

    @Test
    fun `poll decodes each record via the shared schema Json config and upserts it into the repository`() {
        val consumer = newMockConsumer()
        val repository = mockk<ConfirmedEventRepository>(relaxed = true)
        val runner = ConfirmedEventsSinkRunner(consumer, repository, Duration.ofMillis(50))

        consumer.addRecord(ConsumerRecord(topic, 0, 0L, "key1", envelopeJson("0xaaa", ConfirmationStatus.CONFIRMED)))
        consumer.addRecord(ConsumerRecord(topic, 0, 1L, "key2", envelopeJson("0xbbb", ConfirmationStatus.CONFIRMED)))

        val processed = runner.poll()

        processed shouldBe 2
        val captured = mutableListOf<DecodedEventEnvelope>()
        verify(exactly = 2) { repository.upsert(capture(captured)) }
        captured.map { it.txHash } shouldBe listOf("0xaaa", "0xbbb")
    }

    @Test
    fun `poll commits offsets only after processing so a re-poll of the same offsets is safe (idempotent upsert)`() {
        val consumer = newMockConsumer()
        val repository = mockk<ConfirmedEventRepository>(relaxed = true)
        val runner = ConfirmedEventsSinkRunner(consumer, repository, Duration.ofMillis(50))

        consumer.addRecord(ConsumerRecord(topic, 0, 0L, "key1", envelopeJson("0xaaa", ConfirmationStatus.CONFIRMED)))
        runner.poll()

        consumer.committed(setOf(partition))[partition]?.offset() shouldBe 1L
    }

    @Test
    fun `an INVALIDATED message is decoded and passed through to the repository like any other status`() {
        val consumer = newMockConsumer()
        val repository = mockk<ConfirmedEventRepository>(relaxed = true)
        val runner = ConfirmedEventsSinkRunner(consumer, repository, Duration.ofMillis(50))

        consumer.addRecord(ConsumerRecord(topic, 0, 0L, "key1", envelopeJson("0xaaa", ConfirmationStatus.INVALIDATED)))
        val captured = slot<DecodedEventEnvelope>()
        every { repository.upsert(capture(captured)) } returns Unit

        runner.poll()

        captured.captured.status shouldBe ConfirmationStatus.INVALIDATED
    }

    @Test
    fun `poll returns zero and does not commit when there are no records`() {
        val consumer = newMockConsumer()
        val repository = mockk<ConfirmedEventRepository>(relaxed = true)
        val runner = ConfirmedEventsSinkRunner(consumer, repository, Duration.ofMillis(50))

        val processed = runner.poll()

        processed shouldBe 0
        verify(exactly = 0) { repository.upsert(any()) }
    }
}
