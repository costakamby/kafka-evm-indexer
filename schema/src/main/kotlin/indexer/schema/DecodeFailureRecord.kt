package indexer.schema

import kotlinx.serialization.Serializable

/**
 * Value shape for the decoder's dead-letter topic (acceptance criteria 4.3):
 * malformed/undecodable logs carry enough context to debug - the raw log,
 * the contract's ABI reference, and why decoding failed.
 */
@Serializable
data class DecodeFailureRecord(
    val rawLog: RawLogRecord,
    val abiRef: String,
    val reason: String,
    val failedAtEpochMillis: Long,
)
