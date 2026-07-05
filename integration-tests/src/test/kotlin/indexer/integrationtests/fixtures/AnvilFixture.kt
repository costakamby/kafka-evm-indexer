package indexer.integrationtests.fixtures

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import org.web3j.crypto.Credentials
import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService
import java.time.Duration

/**
 * Wraps a Testcontainers-managed Anvil (Foundry) instance forking a real
 * chain at [AnvilFixtureConfig]'s pinned block (design doc section 5.3).
 * Uses one of Anvil's well-known dev accounts for signing.
 */
class AnvilFixture private constructor(
    private val container: GenericContainer<*>,
    val web3j: Web3j,
    val credentials: Credentials,
    val chainId: Long,
    val rpc: AnvilRpcClient,
) : AutoCloseable {

    companion object {
        // Anvil's well-known first dev account - not a secret, safe to hardcode for test fixtures.
        private const val DEV_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        private const val ANVIL_CHAIN_ID = 31337L
        private const val ANVIL_PORT = 8545

        fun start(): AnvilFixture {
            val container = GenericContainer(DockerImageName.parse("ghcr.io/foundry-rs/foundry:latest"))
                .withExposedPorts(ANVIL_PORT)
                .waitingFor(Wait.forLogMessage(".*Listening on 0\\.0\\.0\\.0:$ANVIL_PORT.*\\n", 1))
                .withStartupTimeout(Duration.ofMinutes(2))

            // Entrypoint is `/bin/sh -c`; the whole invocation must be one arg
            // or the shell mis-splits it (each extra Cmd element becomes a
            // positional $N instead of part of the script).
            container.withCreateContainerCmdModifier { cmd ->
                cmd.withEntrypoint("/bin/sh", "-c")
                cmd.withCmd(
                    "anvil --host 0.0.0.0 --fork-url ${AnvilFixtureConfig.forkUrl} " +
                        "--fork-block-number ${AnvilFixtureConfig.forkBlock} --chain-id $ANVIL_CHAIN_ID",
                )
            }
            container.start()

            val rpcUrl = "http://${container.host}:${container.getMappedPort(ANVIL_PORT)}"
            val web3j = Web3j.build(HttpService(rpcUrl))
            val credentials = Credentials.create(DEV_PRIVATE_KEY)
            return AnvilFixture(container, web3j, credentials, ANVIL_CHAIN_ID, AnvilRpcClient(rpcUrl))
        }
    }

    override fun close() {
        web3j.shutdown()
        container.stop()
    }
}
