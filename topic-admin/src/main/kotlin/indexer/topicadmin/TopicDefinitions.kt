package indexer.topicadmin

enum class CleanupPolicy { COMPACT, DELETE }

data class TopicDefinition(
    val name: String,
    val partitions: Int,
    val replicationFactor: Short,
    val cleanupPolicy: CleanupPolicy,
)

/**
 * Explicit, checked-in topic configuration (design doc Phase 0 step 5).
 * subscriptions-topic is compacted (design decision 4: it is the single
 * source of truth for what's watched); the pipeline's event-stream topics
 * use deletion so they don't grow unbounded from replayed history.
 */
object TopicDefinitions {
    val ALL: List<TopicDefinition> = listOf(
        TopicDefinition(
            name = "subscriptions-topic",
            partitions = 3,
            replicationFactor = 1,
            cleanupPolicy = CleanupPolicy.COMPACT,
        ),
        TopicDefinition(
            name = "raw-logs-topic",
            partitions = 6,
            replicationFactor = 1,
            cleanupPolicy = CleanupPolicy.DELETE,
        ),
        TopicDefinition(
            name = "decoded-logs-topic",
            partitions = 6,
            replicationFactor = 1,
            cleanupPolicy = CleanupPolicy.DELETE,
        ),
        TopicDefinition(
            name = "decode-dead-letter-topic",
            partitions = 1,
            replicationFactor = 1,
            cleanupPolicy = CleanupPolicy.DELETE,
        ),
        TopicDefinition(
            name = "confirmed-events-topic",
            partitions = 6,
            replicationFactor = 1,
            cleanupPolicy = CleanupPolicy.DELETE,
        ),
        TopicDefinition(
            name = "reconciliation-anomalies-topic",
            partitions = 3,
            replicationFactor = 1,
            cleanupPolicy = CleanupPolicy.DELETE,
        ),
        // Added by ingestion-poll: durable per-(network, contract) last-polled-block
        // watermark. The design doc identifies "no in-memory-only state that doesn't
        // survive a restart" as a gap for this specific value - a restart without this
        // would either reprocess an unbounded amount of history or silently drop
        // progress. Compacted (like subscriptions-topic) since only the latest value
        // per (network, contract) key ever matters; see
        // indexer.ingestionpoll.progress.KafkaPollProgressStore.
        TopicDefinition(
            name = "poll-progress-topic",
            partitions = 3,
            replicationFactor = 1,
            cleanupPolicy = CleanupPolicy.COMPACT,
        ),
    )
}
