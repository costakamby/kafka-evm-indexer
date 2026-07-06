package indexer.ingestionws.config

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * Unit test (pyramid layer 1): application.yaml loads into a typed AppConfig
 * via Hoplite, mirroring the fleet-wide per-network values (chainId, rpcUrl,
 * confirmationDepth) plus this module's own wsRpcUrl and subscription-API
 * polling config. This module owns its own config class - it does not import
 * subscription-api's AppConfig (would be an inappropriate cross-module
 * dependency per the task brief).
 */
class AppConfigLoaderTest {

    private val config = AppConfigLoader.load()

    @Test
    fun `loads per-network chainId, rpcUrl, wsRpcUrl and confirmationDepth`() {
        val ethereum = config.networks.getValue("ethereum")
        ethereum.chainId shouldBe 1L
        ethereum.rpcUrl shouldBe "https://eth-mainnet.public.blastapi.io"
        ethereum.confirmationDepth shouldBe 12
        ethereum.wsRpcUrl.startsWith("wss://") shouldBe true

        val polygon = config.networks.getValue("polygon")
        polygon.chainId shouldBe 137L
        polygon.confirmationDepth shouldBe 128

        val arbitrum = config.networks.getValue("arbitrum")
        arbitrum.chainId shouldBe 42161L
        arbitrum.confirmationDepth shouldBe 20

        val base = config.networks.getValue("base")
        base.chainId shouldBe 8453L
        base.confirmationDepth shouldBe 20

        val optimism = config.networks.getValue("optimism")
        optimism.chainId shouldBe 10L
        optimism.confirmationDepth shouldBe 20
    }

    @Test
    fun `defaults the subscription API base URL to localhost 8081`() {
        config.subscriptionApi.baseUrl shouldBe "http://localhost:8081"
    }

    @Test
    fun `kafka bootstrap servers and raw-logs-topic are configured`() {
        config.kafka.bootstrapServers shouldBe "localhost:9092"
        config.kafka.rawLogsTopic shouldBe "raw-logs-topic"
    }

    @Test
    fun `WS_RPC_URL_ (NETWORK) env var overrides that network's placeholder wsRpcUrl`() {
        val overridden = AppConfigLoader.load(
            env = mapOf(
                "WS_RPC_URL_ETHEREUM" to "wss://eth-mainnet.g.alchemy.com/v2/real-key",
                "WS_RPC_URL_POLYGON" to "wss://polygon-mainnet.g.alchemy.com/v2/real-key",
            ),
        )

        overridden.networks.getValue("ethereum").wsRpcUrl shouldBe "wss://eth-mainnet.g.alchemy.com/v2/real-key"
        overridden.networks.getValue("polygon").wsRpcUrl shouldBe "wss://polygon-mainnet.g.alchemy.com/v2/real-key"
        // networks with no matching env var keep the checked-in placeholder untouched
        overridden.networks.getValue("arbitrum").wsRpcUrl shouldBe config.networks.getValue("arbitrum").wsRpcUrl
    }

    @Test
    fun `a blank WS_RPC_URL_ (NETWORK) env var is ignored, not applied as an override`() {
        val overridden = AppConfigLoader.load(env = mapOf("WS_RPC_URL_ETHEREUM" to "   "))

        overridden.networks.getValue("ethereum").wsRpcUrl shouldBe config.networks.getValue("ethereum").wsRpcUrl
    }
}
