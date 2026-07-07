package indexer.subscriptionapi.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource

object AppConfigLoader {
    /**
     * Loads application.yaml, then applies `KAFKA_BOOTSTRAP_SERVERS` on top
     * of the checked-in default - the convention shared by the wider Kafka
     * ecosystem's own tooling - so this can point at a real Kafka cluster
     * without editing checked-in config. See the root README's
     * Configuration section. [env] defaults to the real process
     * environment; overridable for tests.
     */
    fun load(env: Map<String, String> = System.getenv()): AppConfig {
        val base = ConfigLoaderBuilder.default()
            .addResourceSource("/application.yaml")
            .build()
            .loadConfigOrThrow<AppConfig>()

        val kafkaBootstrapOverride = env["KAFKA_BOOTSTRAP_SERVERS"]?.takeIf { it.isNotBlank() }
        return base.copy(kafka = base.kafka.copy(bootstrapServers = kafkaBootstrapOverride ?: base.kafka.bootstrapServers))
    }
}
