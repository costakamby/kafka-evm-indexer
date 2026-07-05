package indexer.integrationtests.fixtures

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.web3j.protocol.core.DefaultBlockParameter
import java.math.BigInteger

/**
 * Proves the reusable Anvil fixture library from design doc section 5.3
 * actually works: a pinned-fork Anvil instance, a deployable test ERC20
 * that can emit a near-2^256 value, and a ReorgFixture that mines an
 * alternate block and produces an observable transition. This is the
 * "one deliberate reorg against the fixture library" required by Phase 0
 * step 7 before any Phase 1 subagent is spawned.
 */
class AnvilFixtureAndReorgTest {

    companion object {
        private lateinit var anvil: AnvilFixture
        private lateinit var erc20: Erc20Fixture

        @JvmStatic
        @BeforeAll
        fun setUp() {
            anvil = AnvilFixture.start()
            erc20 = Erc20Fixture(anvil.web3j, anvil.credentials, anvil.chainId)
            erc20.deploy(BigInteger.valueOf(1_000_000))
        }

        @JvmStatic
        @AfterAll
        fun tearDown() {
            anvil.close()
        }
    }

    @Test
    fun `deploying and minting a near 2 to the 256 value succeeds on the pinned fork`() {
        // Leaves headroom below 2^256 - 1 so totalSupply (already seeded with
        // the initial supply from deploy()) doesn't overflow uint256 and revert.
        val nearMax = BigInteger.TWO.pow(256).subtract(BigInteger.valueOf(10_000_000))

        val receipt = erc20.mint(to = anvil.credentials.address, value = nearMax)

        assertTrue(receipt.isStatusOK)
    }

    @Test
    fun `simulateReorg mines two different blocks at the same height and the replacement becomes canonical`() {
        val reorgFixture = ReorgFixture(anvil.web3j, anvil.rpc)
        val recipient = "0x000000000000000000000000000000000000f1"

        val result = reorgFixture.simulateReorg(
            originalAction = { erc20.transfer(recipient, BigInteger.valueOf(111)).transactionHash },
            replacementAction = { erc20.transfer(recipient, BigInteger.valueOf(222)).transactionHash },
        )

        assertNotEquals(result.originalBlockHash, result.replacementBlockHash)
        assertNotEquals(result.originalTxHash, result.replacementTxHash)

        val replacementReceipt = anvil.web3j.ethGetTransactionReceipt(result.replacementTxHash).send().transactionReceipt.get()
        val canonicalBlockAtHeight = anvil.web3j
            .ethGetBlockByNumber(DefaultBlockParameter.valueOf(replacementReceipt.blockNumber), false)
            .send()
            .block

        assertEquals(result.replacementBlockHash, canonicalBlockAtHeight.hash)
        assertNotEquals(result.originalBlockHash, canonicalBlockAtHeight.hash)
    }
}
