package indexer.ingestionpoll.progress

/**
 * Durable per-(network, contract) last-polled-block watermark (design doc:
 * "no in-memory-only state that doesn't survive a restart"). Backed by a
 * small compacted Kafka topic - see KafkaPollProgressStore.
 */
interface PollProgressStore {
    fun lastPolledBlock(network: String, contractAddress: String): Long?

    fun recordProgress(network: String, contractAddress: String, lastPolledBlock: Long)
}
