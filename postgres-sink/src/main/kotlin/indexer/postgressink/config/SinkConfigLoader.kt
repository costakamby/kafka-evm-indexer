package indexer.postgressink.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource

object SinkConfigLoader {
    /**
     * Loads application.yaml, then applies env var overrides on top of the
     * checked-in dev-stack defaults, so this can point at real
     * infrastructure without editing checked-in config - see the root
     * README's Configuration section:
     *  - `KAFKA_BOOTSTRAP_SERVERS` - the convention shared by the wider
     *    Kafka ecosystem's own tooling.
     *  - `POSTGRES_JDBC_URL` / `POSTGRES_USERNAME` / `POSTGRES_PASSWORD`.
     * [env] defaults to the real process environment; overridable for tests.
     */
    fun load(env: Map<String, String> = System.getenv()): SinkConfig {
        val base = ConfigLoaderBuilder.default()
            .addResourceSource("/application.yaml")
            .build()
            .loadConfigOrThrow<SinkConfig>()

        val kafkaBootstrapOverride = env["KAFKA_BOOTSTRAP_SERVERS"]?.takeIf { it.isNotBlank() }
        val jdbcUrlOverride = env["POSTGRES_JDBC_URL"]?.takeIf { it.isNotBlank() }
        val usernameOverride = env["POSTGRES_USERNAME"]?.takeIf { it.isNotBlank() }
        val passwordOverride = env["POSTGRES_PASSWORD"]?.takeIf { it.isNotBlank() }

        return base.copy(
            kafka = base.kafka.copy(bootstrapServers = kafkaBootstrapOverride ?: base.kafka.bootstrapServers),
            postgres = base.postgres.copy(
                jdbcUrl = jdbcUrlOverride ?: base.postgres.jdbcUrl,
                username = usernameOverride ?: base.postgres.username,
                password = passwordOverride ?: base.postgres.password,
            ),
        )
    }
}
