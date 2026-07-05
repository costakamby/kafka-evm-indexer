package indexer.ingestionws.config

/**
 * Per-network config. Mirrors the fleet-wide values (chainId, rpcUrl,
 * confirmationDepth) that every ingestion/subscription module carries for
 * shape-consistency, plus [wsRpcUrl] which is specific to this module: the
 * public per-network RPC URLs shared across the fleet are HTTPS, not WSS, so
 * a real `wss://` endpoint (e.g. an Alchemy/Infura websocket URL) must be
 * supplied here via config/env override for this to work against a real
 * chain - see application.yaml for the placeholder default.
 */
data class NetworkConfig(
    val chainId: Long,
    val rpcUrl: String,
    val wsRpcUrl: String,
    val confirmationDepth: Int,
)

/**
 * Config for reaching the subscription-api module's REST endpoint (owned by
 * another agent's module, built against the exact shared contract:
 * GET {baseUrl}/subscriptions?network=..&status=..). This module owns its
 * own tiny config class for this rather than importing subscription-api's -
 * that would be an inappropriate cross-module dependency.
 */
data class SubscriptionApiConfig(
    val baseUrl: String = "http://localhost:8081",
    val refreshIntervalSeconds: Long = 5,
)

data class KafkaConfig(
    val bootstrapServers: String = "localhost:9092",
    val rawLogsTopic: String = "raw-logs-topic",
)

/** Typed config loaded via Hoplite from application.yaml. */
data class AppConfig(
    val subscriptionApi: SubscriptionApiConfig = SubscriptionApiConfig(),
    val kafka: KafkaConfig = KafkaConfig(),
    val networks: Map<String, NetworkConfig>,
)
