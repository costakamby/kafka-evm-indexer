package indexer.ingestionpoll.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource

object PollAppConfigLoader {
    /**
     * Loads application.yaml, then applies an `RPC_URL_<NETWORK>` env var
     * override per network (e.g. `RPC_URL_ETHEREUM`) on top of the checked-in
     * free public default - this is how a dedicated Alchemy/Infura RPC gets
     * supplied for reliable eth_getLogs polling without editing the
     * checked-in config. [env] defaults to the real process environment;
     * overridable for tests so this doesn't require mutating actual OS
     * environment variables.
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
        return base.copy(networks = overriddenNetworks)
    }
}
