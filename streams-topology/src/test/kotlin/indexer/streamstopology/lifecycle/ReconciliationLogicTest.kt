package indexer.streamstopology.lifecycle

import indexer.schema.ConfirmationStatus
import indexer.schema.DecodedEventEnvelope
import indexer.schema.IngestionSource
import indexer.schema.ReconciliationAnomalyType
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.jupiter.api.Test

/**
 * Pure unit tests (test pyramid layer 1) for the reconciliation merge and
 * anomaly-threshold logic underpinning acceptance criterion 4.4. The actual
 * Kafka wiring (state store persistence, punctuator scheduling) is proven
 * separately by ReconciliationProcessorTest at the TopologyTestDriver layer -
 * these tests isolate the merge/divergence/threshold arithmetic itself.
 */
class ReconciliationLogicTest {

    private fun envelope(source: IngestionSource, value: String = "100", block: Long = 100) = DecodedEventEnvelope(
        eventName = "Transfer",
        signatureHash = "0xtopic0",
        network = "ethereum",
        contractAddress = "0xc",
        txHash = "0xtx",
        logIndex = 0,
        blockNumber = block,
        status = ConfirmationStatus.UNCONFIRMED,
        source = source,
        decodedFields = JsonObject(mapOf("value" to JsonPrimitive(value))),
    )

    @Test
    fun `first sighting from either source creates a single new entry`() {
        val result = ReconciliationLogic.merge(existing = null, incoming = envelope(IngestionSource.POLL), now = 1000L)

        result.divergent shouldBe false
        result.state.seenPoll shouldBe true
        result.state.seenWs shouldBe false
        result.state.firstSeenAtEpochMillis shouldBe 1000L
    }

    @Test
    fun `a corroborating sighting from the other source merges into the SAME entry, not a second one`() {
        val first = ReconciliationLogic.merge(null, envelope(IngestionSource.WS), now = 1000L).state

        val second = ReconciliationLogic.merge(first, envelope(IngestionSource.POLL), now = 2000L)

        second.state.seenWs shouldBe true
        second.state.seenPoll shouldBe true
        // firstSeenAt is preserved from the original sighting, not overwritten.
        second.state.firstSeenAtEpochMillis shouldBe 1000L
        second.divergent shouldBe false
    }

    @Test
    fun `re-delivery from the same source that already corroborated is not flagged divergent`() {
        val first = ReconciliationLogic.merge(null, envelope(IngestionSource.WS), now = 1000L).state
        val second = ReconciliationLogic.merge(first, envelope(IngestionSource.POLL), now = 2000L).state

        // A duplicate poll delivery of the identical decoded content.
        val third = ReconciliationLogic.merge(second, envelope(IngestionSource.POLL), now = 3000L)

        third.divergent shouldBe false
    }

    @Test
    fun `when the corroborating source decodes different fields, it is flagged DIVERGENT_DECODE`() {
        val first = ReconciliationLogic.merge(null, envelope(IngestionSource.WS, value = "100"), now = 1000L).state

        val second = ReconciliationLogic.merge(first, envelope(IngestionSource.POLL, value = "999"), now = 2000L)

        second.divergent shouldBe true
        // Poll is treated as canonical once it corroborates.
        second.state.decodedEvent?.decodedFields?.get("value") shouldBe JsonPrimitive("999")
    }

    @Test
    fun `poll-seen-never-ws is flagged ws_gap_suspected once the gap window elapses`() {
        val state = ReconciliationLogic.merge(null, envelope(IngestionSource.POLL, block = 100), now = 1000L).state

        ReconciliationLogic.checkAnomaly(state, currentBlock = 100, gapBlocks = 3) shouldBe null
        ReconciliationLogic.checkAnomaly(state, currentBlock = 103, gapBlocks = 3) shouldBe ReconciliationAnomalyType.WS_GAP_SUSPECTED
    }

    @Test
    fun `ws-seen-never-corroborated-by-poll is flagged poll_only_confirmed once N blocks elapse`() {
        val state = ReconciliationLogic.merge(null, envelope(IngestionSource.WS, block = 100), now = 1000L).state

        ReconciliationLogic.checkAnomaly(state, currentBlock = 102, gapBlocks = 3) shouldBe null
        ReconciliationLogic.checkAnomaly(state, currentBlock = 103, gapBlocks = 3) shouldBe ReconciliationAnomalyType.POLL_ONLY_CONFIRMED
    }

    @Test
    fun `a fully corroborated entry never raises a gap anomaly no matter how many blocks elapse`() {
        val first = ReconciliationLogic.merge(null, envelope(IngestionSource.WS), now = 1000L).state
        val both = ReconciliationLogic.merge(first, envelope(IngestionSource.POLL), now = 2000L).state

        ReconciliationLogic.checkAnomaly(both, currentBlock = 100_000, gapBlocks = 3) shouldBe null
    }
}
