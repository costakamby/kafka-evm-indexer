package indexer.schema

import indexer.schema.json.IndexerJson
import indexer.schema.ktable.BlockHashEntry
import indexer.schema.ktable.BlockTrackingState
import indexer.schema.ktable.ConfirmationState
import indexer.schema.ktable.ReconciliationState
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.JsonObject
import org.junit.jupiter.api.Test

/**
 * Every topic contract and KTable value shape must round trip through the
 * shared Json config unchanged - this is the stability guarantee every
 * other module builds against (design doc section 2).
 */
class TopicContractsSerializationTest {

    private val json = IndexerJson.instance

    @Test
    fun `SubscriptionRecord round trips`() {
        val record = SubscriptionRecord(
            id = "sub-1",
            network = "ethereum",
            address = "0xcontract",
            abiRef = "erc20-v1",
            startBlock = 100L,
            includeEvents = listOf("Transfer", "Approval"),
            status = SubscriptionStatus.ACTIVE,
            createdAtEpochMillis = 1_700_000_000_000L,
        )

        json.decodeFromString(SubscriptionRecord.serializer(), json.encodeToString(SubscriptionRecord.serializer(), record)) shouldBe record
    }

    @Test
    fun `RawLogRecord round trips`() {
        val record = RawLogRecord(
            network = "ethereum",
            contractAddress = "0xcontract",
            txHash = "0xabc",
            logIndex = 2,
            blockNumber = 100L,
            blockHash = "0xblockhash",
            topics = listOf("0xtopic0", "0xtopic1"),
            data = "0xdata",
            source = IngestionSource.WS,
            observedAtEpochMillis = 1_700_000_000_000L,
        )

        json.decodeFromString(RawLogRecord.serializer(), json.encodeToString(RawLogRecord.serializer(), record)) shouldBe record
    }

    @Test
    fun `ReconciliationAnomaly round trips`() {
        val anomaly = ReconciliationAnomaly(
            type = ReconciliationAnomalyType.WS_GAP_SUSPECTED,
            network = "ethereum",
            txHash = "0xabc",
            logIndex = 2,
            detectedAtEpochMillis = 1_700_000_000_000L,
            details = "poll saw it, ws did not, within 3 blocks",
        )

        json.decodeFromString(ReconciliationAnomaly.serializer(), json.encodeToString(ReconciliationAnomaly.serializer(), anomaly)) shouldBe anomaly
    }

    @Test
    fun `DecodeFailureRecord round trips`() {
        val failure = DecodeFailureRecord(
            rawLog = RawLogRecord(
                network = "ethereum",
                contractAddress = "0xcontract",
                txHash = "0xabc",
                logIndex = 2,
                blockNumber = 100L,
                blockHash = "0xblockhash",
                topics = listOf("0xtopic0"),
                data = "0xdata",
                source = IngestionSource.POLL,
                observedAtEpochMillis = 1_700_000_000_000L,
            ),
            abiRef = "erc20-v1",
            reason = "topic count did not match event signature",
            failedAtEpochMillis = 1_700_000_000_001L,
        )

        json.decodeFromString(DecodeFailureRecord.serializer(), json.encodeToString(DecodeFailureRecord.serializer(), failure)) shouldBe failure
    }

    @Test
    fun `ReconciliationState round trips including a null decodedEvent`() {
        val state = ReconciliationState(
            network = "ethereum",
            txHash = "0xabc",
            logIndex = 2,
            seenWs = true,
            seenPoll = false,
            firstSeenAtEpochMillis = 1_700_000_000_000L,
            decodedEvent = null,
        )

        json.decodeFromString(ReconciliationState.serializer(), json.encodeToString(ReconciliationState.serializer(), state)) shouldBe state
    }

    @Test
    fun `BlockTrackingState round trips`() {
        val state = BlockTrackingState(
            network = "ethereum",
            lastBlock = 100L,
            recentBlockHashes = listOf(
                BlockHashEntry(blockNumber = 99L, blockHash = "0x99"),
                BlockHashEntry(blockNumber = 100L, blockHash = "0x100"),
            ),
            reorgDepthWatermark = 12,
        )

        json.decodeFromString(BlockTrackingState.serializer(), json.encodeToString(BlockTrackingState.serializer(), state)) shouldBe state
    }

    @Test
    fun `ConfirmationState round trips`() {
        val state = ConfirmationState(
            network = "ethereum",
            txHash = "0xabc",
            logIndex = 2,
            status = ConfirmationStatus.CONFIRMED,
            confirmationsSeen = 12,
            decodedEvent = DecodedEventEnvelope(
                eventName = "Transfer",
                signatureHash = "0xsig",
                network = "ethereum",
                contractAddress = "0xcontract",
                txHash = "0xabc",
                logIndex = 2,
                blockNumber = 100L,
                status = ConfirmationStatus.CONFIRMED,
                source = IngestionSource.POLL,
                decodedFields = JsonObject(emptyMap()),
            ),
        )

        json.decodeFromString(ConfirmationState.serializer(), json.encodeToString(ConfirmationState.serializer(), state)) shouldBe state
    }
}
