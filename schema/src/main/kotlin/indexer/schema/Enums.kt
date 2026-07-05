package indexer.schema

import kotlinx.serialization.Serializable

@Serializable
enum class IngestionSource { WS, POLL }

/** Explicit reorg lifecycle per design decision 5. */
@Serializable
enum class ConfirmationStatus { UNCONFIRMED, CONFIRMED, INVALIDATED }

/** First-class reconciliation anomaly kinds per design decision 6. */
@Serializable
enum class ReconciliationAnomalyType { WS_GAP_SUSPECTED, POLL_ONLY_CONFIRMED, DIVERGENT_DECODE }

@Serializable
enum class SubscriptionStatus { ACTIVE, REMOVED }
