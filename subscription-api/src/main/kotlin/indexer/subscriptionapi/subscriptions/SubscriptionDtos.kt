package indexer.subscriptionapi.subscriptions

import kotlinx.serialization.Serializable

/** POST /subscriptions request body (design doc 4.1 - exact field names, contract with ingestion-ws/poll). */
@Serializable
data class CreateSubscriptionRequest(
    val network: String,
    val address: String,
    val abiRef: String,
    val startBlock: Long? = null,
    val includeEvents: List<String> = emptyList(),
)

@Serializable
data class ErrorResponse(val error: String)
