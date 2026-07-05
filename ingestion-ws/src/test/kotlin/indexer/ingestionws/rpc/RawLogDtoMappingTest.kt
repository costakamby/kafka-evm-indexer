package indexer.ingestionws.rpc

import indexer.schema.IngestionSource
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * Unit test (pyramid layer 1): the pure mapping from the wire-format
 * [RawLogDto] (hex fields, as returned by both eth_getLogs and an
 * eth_subscription notification) to the schema's [indexer.schema.RawLogRecord].
 * Proves acceptance criterion 4.2 "tag every raw log with its source and
 * network" at the lowest possible layer - this mapping is called by both the
 * live WS path and the reconnect catch-up path, so getting it right here
 * covers both.
 */
class RawLogDtoMappingTest {

    private val dto = RawLogDto(
        address = "0xabc0000000000000000000000000000000dead",
        blockHash = "0xblockhash",
        blockNumber = "0x64",
        data = "0xdeadbeef",
        logIndex = "0x2",
        removed = false,
        topics = listOf("0xtopic0", "0xtopic1"),
        transactionHash = "0xtxhash",
        transactionIndex = "0x1",
    )

    @Test
    fun `maps hex fields to decimal and tags network and source`() {
        val record = dto.toRawLogRecord(network = "ethereum", source = IngestionSource.WS, observedAtEpochMillis = 42L)

        record.network shouldBe "ethereum"
        record.contractAddress shouldBe "0xabc0000000000000000000000000000000dead"
        record.txHash shouldBe "0xtxhash"
        record.logIndex shouldBe 2L
        record.blockNumber shouldBe 100L
        record.blockHash shouldBe "0xblockhash"
        record.topics shouldBe listOf("0xtopic0", "0xtopic1")
        record.data shouldBe "0xdeadbeef"
        record.source shouldBe IngestionSource.WS
        record.observedAtEpochMillis shouldBe 42L
    }

    @Test
    fun `honors whichever source it is told to tag - ws or poll catch-up`() {
        dto.toRawLogRecord("ethereum", IngestionSource.WS, 1L).source shouldBe IngestionSource.WS
        dto.toRawLogRecord("ethereum", IngestionSource.POLL, 1L).source shouldBe IngestionSource.POLL
    }
}
