package indexer.ingestionws.kafka

import indexer.schema.EventKey
import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord
import indexer.schema.json.IndexerJson
import io.kotest.matchers.shouldBe
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test

/**
 * Unit test (pyramid layer 1): [MockProducer] is an in-memory Kafka producer
 * double shipped in kafka-clients itself, so this proves the key/value
 * encoding contract without a broker (no Testcontainers needed for this
 * tier - a real broker adds nothing this test doesn't already prove).
 */
class KafkaRawLogProducerTest {

    private val mockProducer = MockProducer(true, StringSerializer(), StringSerializer())
    private val producer = KafkaRawLogProducer(mockProducer, topic = "raw-logs-topic")

    private val record = RawLogRecord(
        network = "ethereum",
        contractAddress = "0xabc",
        txHash = "0xtx1",
        logIndex = 3L,
        blockNumber = 100L,
        blockHash = "0xblock",
        topics = listOf("0xtopic0"),
        data = "0xdata",
        source = IngestionSource.WS,
        observedAtEpochMillis = 123L,
    )

    @Test
    fun `sends to raw-logs-topic keyed via EventKey`() {
        producer.send(record)

        mockProducer.history() shouldBe mockProducer.history()
        val sent = mockProducer.history().single()
        sent.topic() shouldBe "raw-logs-topic"
        sent.key() shouldBe EventKey.of("ethereum", "0xtx1", 3L)
    }

    @Test
    fun `value is the RawLogRecord JSON-encoded via the shared IndexerJson config`() {
        producer.send(record)

        val sent = mockProducer.history().single()
        val decoded = IndexerJson.instance.decodeFromString(RawLogRecord.serializer(), sent.value())
        decoded shouldBe record
    }
}
