package indexer.ingestionws.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource

object AppConfigLoader {
    /**
     * Loads application.yaml, then applies env var overrides on top of the
     * checked-in defaults, so this can point at real infrastructure without
     * editing checked-in config - see the root README's Configuration
     * section:
     *  - `WS_RPC_URL_<NETWORK>` overrides wsRpcUrl (eth_subscribe).
     *  - `RPC_URL_<NETWORK>` overrides rpcUrl (used for post-reconnect
     *    eth_getLogs catch-up calls, design decision 7).
     *  - `KAFKA_BOOTSTRAP_SERVERS` overrides kafka.bootstrapServers - the
     *    convention shared by the wider Kafka ecosystem's own tooling
     *    (Kafka Connect, Confluent's Docker images), used here instead of a
     *    Hoplite-native override so it stays instantly recognizable rather
     *    than needing Hoplite's own `config.override.<path>` naming.
     *  - `SUBSCRIPTION_API_BASE_URL` overrides subscriptionApi.baseUrl.
     * [env] defaults to the real process environment; overridable for tests
     * so this doesn't require mutating actual OS environment variables.
     */
    fun load(env: Map<String, String> = System.getenv()): AppConfig {
        val base = ConfigLoaderBuilder.default()
            .addResourceSource("/application.yaml")
            .build()
            .loadConfigOrThrow<AppConfig>()

        val overriddenNetworks = base.networks.mapValues { (network, networkConfig) ->
            val upperNetwork = network.uppercase()
            val rpcOverride = env["RPC_URL_$upperNetwork"]?.takeIf { it.isNotBlank() }
            val wsOverride = env["WS_RPC_URL_$upperNetwork"]?.takeIf { it.isNotBlank() }
            networkConfig.copy(
                rpcUrl = rpcOverride ?: networkConfig.rpcUrl,
                wsRpcUrl = wsOverride ?: networkConfig.wsRpcUrl,
            )
        }

        val kafkaBootstrapOverride = env["KAFKA_BOOTSTRAP_SERVERS"]?.takeIf { it.isNotBlank() }
        val subscriptionApiBaseUrlOverride = env["SUBSCRIPTION_API_BASE_URL"]?.takeIf { it.isNotBlank() }

        return base.copy(
            networks = overriddenNetworks,
            kafka = base.kafka.copy(bootstrapServers = kafkaBootstrapOverride ?: base.kafka.bootstrapServers),
            subscriptionApi = base.subscriptionApi.copy(baseUrl = subscriptionApiBaseUrlOverride ?: base.subscriptionApi.baseUrl),
        )
    }
}
