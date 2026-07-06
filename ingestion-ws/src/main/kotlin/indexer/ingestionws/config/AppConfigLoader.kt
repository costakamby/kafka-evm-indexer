package indexer.ingestionws.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource

object AppConfigLoader {
    /**
     * Loads application.yaml, then applies per-network env var overrides on
     * top of the checked-in defaults - this is how a real Alchemy/Infura
     * endpoint gets supplied without editing the checked-in config (see
     * application.yaml's placeholder note):
     *  - `WS_RPC_URL_<NETWORK>` overrides wsRpcUrl (eth_subscribe).
     *  - `RPC_URL_<NETWORK>` overrides rpcUrl (used for post-reconnect
     *    eth_getLogs catch-up calls, design decision 7).
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
        return base.copy(networks = overriddenNetworks)
    }
}
