package indexer.ingestionws.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource

object AppConfigLoader {
    /**
     * Loads application.yaml, then applies a `WS_RPC_URL_<NETWORK>` env var
     * override per network (e.g. `WS_RPC_URL_ETHEREUM`) on top of the
     * checked-in placeholder wsRpcUrl - this is how a real Alchemy/Infura
     * wss:// endpoint gets supplied without editing the checked-in config
     * (see application.yaml's placeholder note). [env] defaults to the real
     * process environment; overridable for tests so this doesn't require
     * mutating actual OS environment variables.
     */
    fun load(env: Map<String, String> = System.getenv()): AppConfig {
        val base = ConfigLoaderBuilder.default()
            .addResourceSource("/application.yaml")
            .build()
            .loadConfigOrThrow<AppConfig>()

        val overriddenNetworks = base.networks.mapValues { (network, networkConfig) ->
            val override = env["WS_RPC_URL_${network.uppercase()}"]?.takeIf { it.isNotBlank() }
            if (override != null) networkConfig.copy(wsRpcUrl = override) else networkConfig
        }
        return base.copy(networks = overriddenNetworks)
    }
}
