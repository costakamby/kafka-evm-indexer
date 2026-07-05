package indexer.schema

import kotlinx.serialization.Serializable

/** Value shape for reconciliation-anomalies-topic (design decision 6). */
@Serializable
data class ReconciliationAnomaly(
    val type: ReconciliationAnomalyType,
    val network: String,
    val txHash: String,
    val logIndex: Long,
    val detectedAtEpochMillis: Long,
    val details: String,
)
