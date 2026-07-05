package indexer.schema.ktable

import indexer.schema.ConfirmationStatus
import indexer.schema.DecodedEventEnvelope
import kotlinx.serialization.Serializable

/**
 * Confirmation KTable value, keyed by [indexer.schema.EventKey]. Driven by a
 * punctuator tied to block-tracking advances (design doc section 2/3.5).
 */
@Serializable
data class ConfirmationState(
    val network: String,
    val txHash: String,
    val logIndex: Long,
    val status: ConfirmationStatus,
    val confirmationsSeen: Int,
    val decodedEvent: DecodedEventEnvelope,
)
