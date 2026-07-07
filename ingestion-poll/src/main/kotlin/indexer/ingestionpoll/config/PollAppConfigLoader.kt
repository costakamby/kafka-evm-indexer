package indexer.ingestionpoll.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource

object PollAppConfigLoader {
    /**
     * Loads application.yaml, then applies env var overrides on top of the
     * checked-in defaults, so this can point at real infrastructure without
     * editing checked-in config - see the root README's Configuration
     * section:
     *  - `RPC_URL_<NETWORK>` overrides that network's rpcUrl.
     *  - `KAFKA_BOOTSTRAP_SERVERS` overrides poll.kafkaBootstrapServers - the
     *    convention shared by the wider Kafka ecosystem's own tooling.
     *  - `SUBSCRIPTION_API_BASE_URL` overrides poll.subscriptionApiBaseUrl.
     * [env] defaults to the real process environment; overridable for tests
     * so this doesn't require mutating actual OS environment variables.
     */
    fun load(env: Map<String, String> = System.getenv()): PollAppConfig {
        val base = ConfigLoaderBuilder.default()
            .addResourceSource("/application.yaml")
            .build()
            .loadConfigOrThrow<PollAppConfig>()

        val overriddenNetworks = base.networks.mapValues { (network, networkConfig) ->
            val override = env["RPC_URL_${network.uppercase()}"]?.takeIf { it.isNotBlank() }
            if (override != null) networkConfig.copy(rpcUrl = override) else networkConfig
        }

        val kafkaBootstrapOverride = env["KAFKA_BOOTSTRAP_SERVERS"]?.takeIf { it.isNotBlank() }
        val subscriptionApiBaseUrlOverride = env["SUBSCRIPTION_API_BASE_URL"]?.takeIf { it.isNotBlank() }

        return base.copy(
            networks = overriddenNetworks,
            poll = base.poll.copy(
                kafkaBootstrapServers = kafkaBootstrapOverride ?: base.poll.kafkaBootstrapServers,
                subscriptionApiBaseUrl = subscriptionApiBaseUrlOverride ?: base.poll.subscriptionApiBaseUrl,
            ),
        )
    }
}
