package indexer.streamstopology.lifecycle

import indexer.schema.DecodedEventEnvelope
import indexer.schema.IngestionSource
import indexer.schema.ReconciliationAnomalyType
import indexer.schema.ktable.ReconciliationState

data class ReconciliationMergeResult(
    val state: ReconciliationState,
    /** True the moment the corroborating source's decode disagrees with the first source's. */
    val divergent: Boolean,
)

/**
 * Pure merge/anomaly-threshold logic for the reconciliation KTable (acceptance
 * criterion 4.4): an event seen by both ws and poll for the same key merges
 * into exactly one entry (never two competing ones), and one-sided sightings
 * age out into an explicit anomaly rather than staying silent forever.
 *
 * Anomaly-type mapping (the design doc leaves the exact name "TBD by
 * implementer" for the ws-seen/poll-missing case, while fixing the enum's 3
 * values already in schema - no new enum values were added):
 *  - poll saw it, ws never did  -> WS_GAP_SUSPECTED (a gap in ws coverage is suspected)
 *  - ws saw it, poll never corroborated within the window -> POLL_ONLY_CONFIRMED
 *    (per design decision 3, "poll is required for CONFIRMED status" - this
 *    event's path to confirmation depends solely on poll corroborating it)
 *  - both saw it but decoded different field content -> DIVERGENT_DECODE
 */
object ReconciliationLogic {

    fun merge(existing: ReconciliationState?, incoming: DecodedEventEnvelope, now: Long): ReconciliationMergeResult {
        if (existing == null) {
            return ReconciliationMergeResult(
                state = ReconciliationState(
                    network = incoming.network,
                    txHash = incoming.txHash,
                    logIndex = incoming.logIndex,
                    seenWs = incoming.source == IngestionSource.WS,
                    seenPoll = incoming.source == IngestionSource.POLL,
                    firstSeenAtEpochMillis = now,
                    decodedEvent = incoming,
                ),
                divergent = false,
            )
        }

        val isCorroboratingArrival = (incoming.source == IngestionSource.WS && !existing.seenWs) ||
            (incoming.source == IngestionSource.POLL && !existing.seenPoll)

        val existingDecoded = existing.decodedEvent
        val divergent = isCorroboratingArrival &&
            existingDecoded != null &&
            existingDecoded.decodedFields != incoming.decodedFields

        // Poll is treated as canonical once available (design decision 3: poll
        // is the deterministic, required-for-confirmation source).
        val canonicalDecoded = if (incoming.source == IngestionSource.POLL) incoming else (existing.decodedEvent ?: incoming)

        val merged = existing.copy(
            seenWs = existing.seenWs || incoming.source == IngestionSource.WS,
            seenPoll = existing.seenPoll || incoming.source == IngestionSource.POLL,
            decodedEvent = canonicalDecoded,
        )
        return ReconciliationMergeResult(merged, divergent)
    }

    /** Null if no anomaly should be raised (yet). Never re-raises once both sources have corroborated. */
    fun checkAnomaly(state: ReconciliationState, currentBlock: Long, gapBlocks: Int): ReconciliationAnomalyType? {
        val eventBlock = state.decodedEvent?.blockNumber ?: return null
        val blocksElapsed = currentBlock - eventBlock
        if (blocksElapsed < gapBlocks) return null

        return when {
            state.seenPoll && !state.seenWs -> ReconciliationAnomalyType.WS_GAP_SUSPECTED
            state.seenWs && !state.seenPoll -> ReconciliationAnomalyType.POLL_ONLY_CONFIRMED
            else -> null
        }
    }
}
