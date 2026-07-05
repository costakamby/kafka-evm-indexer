package indexer.subscriptionapi.config

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * application.yaml is loaded via Hoplite into a typed AppConfig with a
 * per-network map carrying at least chainId, rpcUrl, confirmationDepth
 * (design doc Phase 0 step 4), matching the per-network confirmation
 * depth defaults from design decision 11.
 */
class AppConfigLoaderTest {

    @Test
    fun `loads per-network config with chainId, rpcUrl and confirmationDepth`() {
        val config = AppConfigLoader.load()

        val ethereum = config.networks.getValue("ethereum")
        ethereum.chainId shouldBe 1L
        ethereum.confirmationDepth shouldBe 12

        val polygon = config.networks.getValue("polygon")
        polygon.chainId shouldBe 137L
        polygon.confirmationDepth shouldBe 128

        val arbitrum = config.networks.getValue("arbitrum")
        arbitrum.confirmationDepth shouldBe 20
    }

    @Test
    fun `server config has a port`() {
        val config = AppConfigLoader.load()

        config.server.port shouldBe 8081
    }
}
