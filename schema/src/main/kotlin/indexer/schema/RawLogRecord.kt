package indexer.schema

import kotlinx.serialization.Serializable

/** Value shape for raw-logs-topic, partitioned by network. */
@Serializable
data class RawLogRecord(
    val network: String,
    val contractAddress: String,
    val txHash: String,
    val logIndex: Long,
    val blockNumber: Long,
    val blockHash: String,
    val topics: List<String>,
    val data: String,
    val source: IngestionSource,
    val observedAtEpochMillis: Long,
)
