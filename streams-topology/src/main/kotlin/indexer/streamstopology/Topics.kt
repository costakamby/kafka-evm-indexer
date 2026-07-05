package indexer.streamstopology

/**
 * Topic name constants for the single Kafka Streams topology built by this
 * module. Must match [indexer.topicadmin.TopicDefinitions] exactly - kept as
 * plain string constants here (rather than a dependency on topic-admin) since
 * this module only needs the names, not the provisioning logic.
 */
object Topics {
    const val SUBSCRIPTIONS = "subscriptions-topic"
    const val RAW_LOGS = "raw-logs-topic"
    const val DECODED_LOGS = "decoded-logs-topic"
    const val DECODE_DEAD_LETTER = "decode-dead-letter-topic"
    const val CONFIRMED_EVENTS = "confirmed-events-topic"
    const val RECONCILIATION_ANOMALIES = "reconciliation-anomalies-topic"

    /**
     * Name of the GlobalKTable state store materializing subscriptions-topic,
     * keyed by subscription id - queried directly by subscription-api's REST
     * layer via Interactive Queries (acceptance criterion 4.1). Every instance
     * holds this fully replicated, so any instance can answer any query.
     */
    const val SUBSCRIPTIONS_STORE = "subscriptions-global-store"
}
