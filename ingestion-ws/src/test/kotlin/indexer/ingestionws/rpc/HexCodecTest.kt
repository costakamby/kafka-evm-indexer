package indexer.ingestionws.rpc

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * Unit tests (pyramid layer 1, design doc 5.2): pure hex<->Long conversion
 * used to parse the hex-encoded blockNumber/logIndex fields the Ethereum
 * JSON-RPC wire format always uses.
 */
class HexCodecTest {

    @Test
    fun `toLong parses a 0x-prefixed hex string`() {
        HexCodec.toLong("0x64") shouldBe 100L
        HexCodec.toLong("0x0") shouldBe 0L
        HexCodec.toLong("0x1e8480") shouldBe 2000000L
    }

    @Test
    fun `toHex renders a Long as a 0x-prefixed hex string`() {
        HexCodec.toHex(100L) shouldBe "0x64"
        HexCodec.toHex(0L) shouldBe "0x0"
        HexCodec.toHex(2000000L) shouldBe "0x1e8480"
    }

    @Test
    fun `toHex and toLong round-trip`() {
        val values = listOf(0L, 1L, 42L, 123456789L, Long.MAX_VALUE / 2)
        values.forEach { value ->
            HexCodec.toLong(HexCodec.toHex(value)) shouldBe value
        }
    }
}
