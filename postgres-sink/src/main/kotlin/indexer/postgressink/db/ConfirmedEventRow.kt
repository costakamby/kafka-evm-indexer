package indexer.postgressink.db

import indexer.schema.ConfirmationStatus
import indexer.schema.IngestionSource
import kotlinx.serialization.json.JsonObject

/**
 * A materialized confirmed_events row, deliberately excluding id/created_at/
 * updated_at - those are storage bookkeeping, not part of the logical state
 * that idempotent replay is expected to reproduce identically.
 */
data class ConfirmedEventRow(
    val network: String,
    val txHash: String,
    val logIndex: Long,
    val eventName: String,
    val signatureHash: String,
    val contractAddress: String,
    val blockNumber: Long,
    val status: ConfirmationStatus,
    val source: IngestionSource,
    val decodedFields: JsonObject,
)
