package indexer.decoder

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * abiRef resolution + validation (test pyramid layer 1). Traces to acceptance
 * criterion 4.1: a malformed/unknown abiRef must be rejectable at the API layer.
 */
class AbiRegistryTest {

    private val registry = AbiRegistry()

    @Test
    fun `a known checked-in abiRef resolves and is valid`() {
        registry.isValid("erc20") shouldBe true
        val abi = registry.resolve("erc20")
        abi.events.map { it.name }.toSet() shouldBe setOf("Transfer", "Approval")
    }

    @Test
    fun `Transfer signature hash matches the canonical ERC20 topic0`() {
        val abi = registry.resolve("erc20")
        val transfer = abi.events.single { it.name == "Transfer" }
        transfer.canonicalSignature shouldBe "Transfer(address,address,uint256)"
        val topic0 = abi.eventsByTopic0.entries.single { it.value == transfer }.key
        topic0 shouldBe "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    }

    @Test
    fun `an unknown abiRef is not valid`() {
        registry.isValid("no-such-abi") shouldBe false
    }

    @Test
    fun `an abiRef pointing at malformed ABI JSON is not valid`() {
        registry.isValid("broken") shouldBe false
    }
}
