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
}
