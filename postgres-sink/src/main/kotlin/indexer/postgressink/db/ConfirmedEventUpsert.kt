package indexer.postgressink.db

import indexer.schema.DecodedEventEnvelope
import indexer.schema.json.IndexerJson
import kotlinx.serialization.json.JsonObject

/**
 * Pure SQL-building logic for materializing confirmed-events-topic into
 * Postgres (design doc 4.7). Upserts on the (network, tx_hash, log_index)
 * unique constraint so that:
 *  - replaying confirmed-events-topic from offset 0 into an empty Postgres
 *    is idempotent (re-applying the same envelope is a no-op change), and
 *  - an INVALIDATED correction message updates the existing row's status
 *    (and every other column) in place, rather than leaving a stale
 *    CONFIRMED row behind or inserting a duplicate.
 *
 * decodedFields is stored as JSONB (schema module design: it's an open,
 * ABI-dependent blob with no fixed shape, not something to normalize into
 * typed columns). All other envelope fields are normal typed columns.
 */
object ConfirmedEventUpsert {

    val SQL: String = """
        INSERT INTO confirmed_events (
            network, tx_hash, log_index, event_name, signature_hash,
            contract_address, block_number, status, source, decoded_fields
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
        ON CONFLICT (network, tx_hash, log_index)
        DO UPDATE SET
            event_name = EXCLUDED.event_name,
            signature_hash = EXCLUDED.signature_hash,
            contract_address = EXCLUDED.contract_address,
            block_number = EXCLUDED.block_number,
            status = EXCLUDED.status,
            source = EXCLUDED.source,
            decoded_fields = EXCLUDED.decoded_fields,
            updated_at = now()
    """.trimIndent()

    /** Ordered params matching the `?` placeholders in [SQL], in order. */
    fun paramsFor(envelope: DecodedEventEnvelope): List<Any?> = listOf(
        envelope.network,
        envelope.txHash,
        envelope.logIndex,
        envelope.eventName,
        envelope.signatureHash,
        envelope.contractAddress,
        envelope.blockNumber,
        envelope.status.name,
        envelope.source.name,
        IndexerJson.instance.encodeToString(JsonObject.serializer(), envelope.decodedFields),
    )
}
