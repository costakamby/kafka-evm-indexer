package indexer.schema

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

/**
 * Shared envelope for decoded-logs-topic and confirmed-events-topic: fixed,
 * controlled fields plus a decodedFields blob whose shape is dictated by
 * whatever ABI produced it (design doc section 1). Every BigInteger placed
 * into [decodedFields] must go through [indexer.schema.json.bigIntegerJsonField].
 */
@Serializable
data class DecodedEventEnvelope(
    val eventName: String,
    val signatureHash: String,
    val network: String,
    val contractAddress: String,
    val txHash: String,
    val logIndex: Long,
    val blockNumber: Long,
    val status: ConfirmationStatus,
    val source: IngestionSource,
    val decodedFields: JsonObject,
)
