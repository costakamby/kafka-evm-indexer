package indexer.schema.ktable

import indexer.schema.DecodedEventEnvelope
import kotlinx.serialization.Serializable

/**
 * Reconciliation KTable value, keyed by [indexer.schema.EventKey]
 * (design doc section 2/3.3).
 */
@Serializable
data class ReconciliationState(
    val network: String,
    val txHash: String,
    val logIndex: Long,
    val seenWs: Boolean,
    val seenPoll: Boolean,
    val firstSeenAtEpochMillis: Long,
    val decodedEvent: DecodedEventEnvelope? = null,
)
