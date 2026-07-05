package indexer.ingestionpoll.progress

import kotlinx.serialization.Serializable

/**
 * Value shape for this module's own poll-progress-topic - private state
 * internal to ingestion-poll (not a cross-module contract), so it lives here
 * rather than in the shared `schema` module.
 */
@Serializable
data class PollProgressRecord(
    val network: String,
    val contractAddress: String,
    val lastPolledBlock: Long,
    val updatedAtEpochMillis: Long,
)
