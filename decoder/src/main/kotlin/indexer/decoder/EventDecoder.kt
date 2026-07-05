package indexer.decoder

import indexer.schema.ConfirmationStatus
import indexer.schema.DecodeFailureRecord
import indexer.schema.DecodedEventEnvelope
import indexer.schema.RawLogRecord
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/** Outcome of decoding a single raw log. */
sealed interface DecodeResult {
    data class Success(val envelope: DecodedEventEnvelope) : DecodeResult
    data class Failure(val record: DecodeFailureRecord) : DecodeResult
}

/**
 * Decodes a [RawLogRecord] against the ABI resolved from its subscription's
 * abiRef + the log's topic0 into a [DecodedEventEnvelope]. Any malformed or
 * undecodable log becomes a [DecodeResult.Failure] carrying a
 * [DecodeFailureRecord] with enough context to debug (raw log + abiRef + reason)
 * — never dropped silently, never allowed to throw out of this method
 * (acceptance criterion 4.3).
 */
class EventDecoder(
    private val registry: AbiRegistry,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    fun decode(raw: RawLogRecord, abiRef: String): DecodeResult {
        return try {
            val abi = registry.resolve(abiRef)

            val topic0 = raw.topics.firstOrNull()
                ?: return failure(raw, abiRef, "log has no topics (anonymous events unsupported)")
            val event = abi.eventsByTopic0[topic0]
                ?: return failure(raw, abiRef, "no event in abiRef '$abiRef' matches topic0 $topic0")

            val indexedParams = event.params.filter { it.indexed }
            val nonIndexedParams = event.params.filterNot { it.indexed }

            val indexedTopics = raw.topics.drop(1)
            if (indexedTopics.size != indexedParams.size) {
                return failure(
                    raw,
                    abiRef,
                    "topic count mismatch for ${event.name}: expected ${indexedParams.size} indexed " +
                        "topic(s), got ${indexedTopics.size}",
                )
            }

            val decoded = LinkedHashMap<String, JsonElement>()
            indexedParams.forEachIndexed { i, param ->
                decoded[param.name] = AbiDecoder.decodeIndexedTopic(indexedTopics[i], param.type)
            }
            val dataBytes = AbiDecoder.hexToBytes(raw.data)
            val nonIndexedValues = AbiDecoder.decodeData(
                dataBytes,
                nonIndexedParams.map { Component(it.name, it.type) },
            )
            // Preserve the ABI's declared parameter order in the output.
            event.params.forEach { param ->
                if (!param.indexed) decoded[param.name] = nonIndexedValues.getValue(param.name)
            }

            DecodeResult.Success(
                DecodedEventEnvelope(
                    eventName = event.name,
                    signatureHash = topic0,
                    network = raw.network,
                    contractAddress = raw.contractAddress,
                    txHash = raw.txHash,
                    logIndex = raw.logIndex,
                    blockNumber = raw.blockNumber,
                    status = ConfirmationStatus.UNCONFIRMED,
                    source = raw.source,
                    decodedFields = JsonObject(decoded),
                ),
            )
        } catch (e: Exception) {
            failure(raw, abiRef, "decode error: ${e.message ?: e::class.simpleName}")
        }
    }

    private fun failure(raw: RawLogRecord, abiRef: String, reason: String): DecodeResult.Failure =
        DecodeResult.Failure(
            DecodeFailureRecord(
                rawLog = raw,
                abiRef = abiRef,
                reason = reason,
                failedAtEpochMillis = clock(),
            ),
        )
}
