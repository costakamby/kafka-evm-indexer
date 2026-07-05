package indexer.subscriptionapi.config

data class NetworkConfig(
    val chainId: Long,
    val rpcUrl: String,
    val confirmationDepth: Int,
)

data class ServerConfig(
    val port: Int,
)

data class KafkaConfig(
    val bootstrapServers: String,
    val applicationId: String,
)

/** Typed config loaded via Hoplite from application.yaml (design doc Phase 0 step 4). */
data class AppConfig(
    val server: ServerConfig,
    val kafka: KafkaConfig,
    val networks: Map<String, NetworkConfig>,
)
