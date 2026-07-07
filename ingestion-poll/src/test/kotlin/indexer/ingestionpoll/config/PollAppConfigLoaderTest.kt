package indexer.ingestionpoll.config

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * application.yaml is loaded via Hoplite into a typed PollAppConfig carrying
 * the fleet-wide per-network values (chainId/rpcUrl/confirmationDepth, must
 * mirror every other module) plus this module's own poll-specific settings
 * (maxBlockRange, pollIntervalMs, retry/backoff, subscriptionApiBaseUrl).
 * Layer 1 of the test pyramid (design doc section 5.2) - pure config parsing,
 * no network/broker involved.
 */
class PollAppConfigLoaderTest {

    @Test
    fun `loads per-network config mirroring the fleet-wide defaults`() {
        val config = PollAppConfigLoader.load()

        val ethereum = config.networks.getValue("ethereum")
        ethereum.chainId shouldBe 1L
        ethereum.rpcUrl shouldBe "https://eth-mainnet.public.blastapi.io"
        ethereum.confirmationDepth shouldBe 12

        val arbitrum = config.networks.getValue("arbitrum")
        arbitrum.chainId shouldBe 42161L
        arbitrum.confirmationDepth shouldBe 20

        val base = config.networks.getValue("base")
        base.chainId shouldBe 8453L
        base.confirmationDepth shouldBe 20

        val optimism = config.networks.getValue("optimism")
        optimism.chainId shouldBe 10L
        optimism.confirmationDepth shouldBe 20

        val polygon = config.networks.getValue("polygon")
        polygon.chainId shouldBe 137L
        polygon.confirmationDepth shouldBe 128
    }

    @Test
    fun `loads poll-specific settings with a default subscription api base url`() {
        val config = PollAppConfigLoader.load()

        config.poll.subscriptionApiBaseUrl shouldBe "http://localhost:8081"
        config.poll.maxBlockRange shouldBe 2000L
        config.poll.pollIntervalMs shouldBe 15000L
        config.poll.maxRetries shouldBe 5
        config.poll.initialBackoffMs shouldBe 200L
        config.poll.maxBackoffMs shouldBe 10000L
        config.poll.kafkaBootstrapServers shouldBe "localhost:9092"
    }

    @Test
    fun `RPC_URL_ (NETWORK) env var overrides that network's checked-in rpcUrl`() {
        val default = PollAppConfigLoader.load()

        val overridden = PollAppConfigLoader.load(
            env = mapOf(
                "RPC_URL_ETHEREUM" to "https://eth-mainnet.g.alchemy.com/v2/real-key",
                "RPC_URL_POLYGON" to "https://polygon-mainnet.g.alchemy.com/v2/real-key",
            ),
        )

        overridden.networks.getValue("ethereum").rpcUrl shouldBe "https://eth-mainnet.g.alchemy.com/v2/real-key"
        overridden.networks.getValue("polygon").rpcUrl shouldBe "https://polygon-mainnet.g.alchemy.com/v2/real-key"
        // networks with no matching env var keep the checked-in default untouched
        overridden.networks.getValue("arbitrum").rpcUrl shouldBe default.networks.getValue("arbitrum").rpcUrl
    }

    @Test
    fun `a blank RPC_URL_ (NETWORK) env var is ignored, not applied as an override`() {
        val default = PollAppConfigLoader.load()

        val overridden = PollAppConfigLoader.load(env = mapOf("RPC_URL_ETHEREUM" to "   "))

        overridden.networks.getValue("ethereum").rpcUrl shouldBe default.networks.getValue("ethereum").rpcUrl
    }

    @Test
    fun `KAFKA_BOOTSTRAP_SERVERS env var overrides the checked-in default, for bring-your-own-Kafka setups`() {
        val overridden = PollAppConfigLoader.load(env = mapOf("KAFKA_BOOTSTRAP_SERVERS" to "my-broker:9092"))

        overridden.poll.kafkaBootstrapServers shouldBe "my-broker:9092"
    }

    @Test
    fun `a blank KAFKA_BOOTSTRAP_SERVERS env var is ignored, not applied as an override`() {
        val default = PollAppConfigLoader.load()

        val overridden = PollAppConfigLoader.load(env = mapOf("KAFKA_BOOTSTRAP_SERVERS" to "  "))

        overridden.poll.kafkaBootstrapServers shouldBe default.poll.kafkaBootstrapServers
    }

    @Test
    fun `SUBSCRIPTION_API_BASE_URL env var overrides the checked-in default`() {
        val overridden = PollAppConfigLoader.load(env = mapOf("SUBSCRIPTION_API_BASE_URL" to "http://my-subscription-api:9000"))

        overridden.poll.subscriptionApiBaseUrl shouldBe "http://my-subscription-api:9000"
    }
}
