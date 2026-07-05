package indexer.postgressink.config

data class KafkaSinkConfig(
    val bootstrapServers: String,
    val topic: String,
    val groupId: String,
    val pollTimeoutMs: Long,
)

data class PostgresConfig(
    val jdbcUrl: String,
    val username: String,
    val password: String,
)

/** Typed config loaded via Hoplite from application.yaml (mirrors subscription-api's pattern). */
data class SinkConfig(
    val kafka: KafkaSinkConfig,
    val postgres: PostgresConfig,
)
