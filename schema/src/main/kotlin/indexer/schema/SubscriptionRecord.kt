package indexer.schema

import kotlinx.serialization.Serializable

/**
 * Value shape for subscriptions-topic (compacted, keyed by [id]). This is
 * the single source of truth for what's being watched (design decision 4) -
 * a REMOVED status record is the tombstone-adjacent way of turning off a
 * subscription without a compaction tombstone losing history in Kafbat UI.
 */
@Serializable
data class SubscriptionRecord(
    val id: String,
    val network: String,
    val address: String,
    val abiRef: String,
    val startBlock: Long? = null,
    val includeEvents: List<String> = emptyList(),
    val status: SubscriptionStatus,
    val createdAtEpochMillis: Long,
)
