package indexer.ingestionpoll.config

/** Fleet-wide per-network values - must mirror every other module's application.yaml exactly. */
data class NetworkConfig(
    val chainId: Long,
    val rpcUrl: String,
    val confirmationDepth: Int,
)

/**
 * This module's own poll-specific settings (design doc 4.2: "REST poller
 * respects configurable batch size / block range per call and handles
 * provider rate-limit errors with backoff").
 */
data class PollSettings(
    /** Base URL of the subscription-api's REST endpoint this poller reads active subscriptions from. */
    val subscriptionApiBaseUrl: String = "http://localhost:8081",
    /** How often (ms) each network re-fetches active subscriptions and polls eth_getLogs. */
    val pollIntervalMs: Long = 15_000,
    /** Maximum number of blocks requested in a single eth_getLogs call. */
    val maxBlockRange: Long = 2_000,
    /** Maximum retry attempts for a single block range before giving up on that cycle. */
    val maxRetries: Int = 5,
    /** Initial exponential backoff delay (ms) after a rate-limit response. */
    val initialBackoffMs: Long = 200,
    /** Backoff delay cap (ms). */
    val maxBackoffMs: Long = 10_000,
    /** Kafka bootstrap servers this instance produces raw logs / poll-progress to. */
    val kafkaBootstrapServers: String = "localhost:9092",
)

/** Typed config loaded via Hoplite from this module's own application.yaml. */
data class PollAppConfig(
    val networks: Map<String, NetworkConfig>,
    val poll: PollSettings = PollSettings(),
)
