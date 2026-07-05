package indexer.streamstopology.serde

import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/** Pure unit test (pyramid layer 1): the generic Serde round-trips a schema type. */
class JsonSerdeTest {

    @Test
    fun `RawLogRecord round trips through the generic JsonSerde`() {
        val serde = jsonSerdeOf(RawLogRecord.serializer())
        val record = RawLogRecord(
            network = "ethereum",
            contractAddress = "0xabc",
            txHash = "0xtx",
            logIndex = 1,
            blockNumber = 100,
            blockHash = "0xblock",
            topics = listOf("0xtopic0"),
            data = "0x00",
            source = IngestionSource.WS,
            observedAtEpochMillis = 42,
        )

        val bytes = serde.serializer().serialize("raw-logs-topic", record)
        val roundTripped = serde.deserializer().deserialize("raw-logs-topic", bytes)

        roundTripped shouldBe record
    }
}
