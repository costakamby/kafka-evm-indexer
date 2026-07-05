package indexer.ingestionpoll.poll

import indexer.ingestionpoll.rpc.RawLog
import indexer.schema.IngestionSource
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * Pure mapping from the RPC layer's RawLog into the schema's RawLogRecord,
 * tagging source=POLL and the network (design doc 4.2: "Both ingestion
 * paths correctly tag every raw log with its source (ws or poll) and
 * network" - this is the poll side). Layer 1 of the test pyramid.
 */
class RawLogMapperTest {

    @Test
    fun `maps every field and tags source=POLL and the given network`() {
        val rawLog = RawLog(
            address = "0xabc",
            topics = listOf("0xTOPIC0"),
            data = "0xdead",
            blockNumber = 42,
            blockHash = "0xblockhash",
            transactionHash = "0xtxhash",
            logIndex = 3,
        )

        val record = RawLogMapper.toRawLogRecord(network = "ethereum", log = rawLog, observedAtEpochMillis = 12345L)

        record.network shouldBe "ethereum"
        record.contractAddress shouldBe "0xabc"
        record.txHash shouldBe "0xtxhash"
        record.logIndex shouldBe 3L
        record.blockNumber shouldBe 42L
        record.blockHash shouldBe "0xblockhash"
        record.topics shouldBe listOf("0xTOPIC0")
        record.data shouldBe "0xdead"
        record.source shouldBe IngestionSource.POLL
        record.observedAtEpochMillis shouldBe 12345L
    }
}
