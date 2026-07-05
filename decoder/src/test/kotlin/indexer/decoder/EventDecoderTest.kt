package indexer.decoder

import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test
import org.web3j.abi.DefaultFunctionEncoder
import org.web3j.abi.datatypes.Address
import org.web3j.abi.datatypes.Bool
import org.web3j.abi.datatypes.DynamicArray
import org.web3j.abi.datatypes.StaticStruct
import org.web3j.abi.datatypes.Utf8String
import org.web3j.abi.datatypes.generated.Int256
import org.web3j.abi.datatypes.generated.Uint256
import java.math.BigInteger

/**
 * Decoder unit tests (test pyramid layer 1: pure, no Kafka/network).
 * Traces to acceptance criteria 4.3 and design decision 10 (numeric rule).
 */
class EventDecoderTest {

    private val registry = AbiRegistry()
    private val decoder = EventDecoder(registry, clock = { 1_700_000_000_000L })

    private val transferTopic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    private fun addressTopic(hex: String): String {
        val clean = hex.removePrefix("0x").lowercase()
        return "0x" + "0".repeat(64 - clean.length) + clean
    }

    private fun word(value: BigInteger): String {
        val hex = value.toString(16)
        return "0x" + "0".repeat(64 - hex.length) + hex
    }

    @Test
    fun `erc20 Transfer decodes with the value field as a JSON string not a number`() {
        val nearMax = BigInteger.TWO.pow(256).subtract(BigInteger.ONE)
        val raw = RawLogRecord(
            network = "ethereum",
            contractAddress = "0x00000000000000000000000000000000000000aa",
            txHash = "0xtx",
            logIndex = 5,
            blockNumber = 100,
            blockHash = "0xblock",
            topics = listOf(
                transferTopic0,
                addressTopic("0x00000000000000000000000000000000000000de"),
                addressTopic("0x00000000000000000000000000000000000000be"),
            ),
            data = word(nearMax),
            source = IngestionSource.POLL,
            observedAtEpochMillis = 1,
        )

        val result = decoder.decode(raw, "erc20")

        result.shouldBeInstanceOf<DecodeResult.Success>()
        val fields = result.envelope.decodedFields
        result.envelope.eventName shouldBe "Transfer"
        result.envelope.signatureHash shouldBe transferTopic0
        // The near-2^256 value must be a JSON STRING, never a raw numeric literal.
        val value = fields.getValue("value").jsonPrimitive
        value.isString shouldBe true
        value.content shouldBe nearMax.toString()
        fields.getValue("from").jsonPrimitive.content shouldBe "0x00000000000000000000000000000000000000de"
    }

    @Test
    fun `tiny uint value also serializes as a JSON string`() {
        val raw = RawLogRecord(
            "ethereum", "0xaa", "0xtx", 0, 1, "0xb",
            topics = listOf(
                transferTopic0,
                addressTopic("0x01"),
                addressTopic("0x02"),
            ),
            data = word(BigInteger.ONE),
            source = IngestionSource.WS,
            observedAtEpochMillis = 1,
        )

        val result = decoder.decode(raw, "erc20")
        result.shouldBeInstanceOf<DecodeResult.Success>()
        val value = result.envelope.decodedFields.getValue("value").jsonPrimitive
        value.isString shouldBe true
        value.content shouldBe "1"
    }

    @Test
    fun `nested tuple and struct fields decode recursively with every numeric leaf as a string`() {
        // OrderFilled(address indexed maker, (uint256 id,(address token,int256 delta),bool active) order,
        //             uint256[] amounts, string memo)
        val order = StaticStruct(
            Uint256(BigInteger.valueOf(7)),
            StaticStruct(
                Address("0x00000000000000000000000000000000000000ab"),
                Int256(BigInteger.valueOf(-5)),
            ),
            Bool(true),
        )
        val amounts = DynamicArray(
            Uint256::class.java,
            Uint256(BigInteger.ONE),
            Uint256(BigInteger.TWO),
            Uint256(BigInteger.TEN),
        )
        val memo = Utf8String("hello")
        val data = "0x" + DefaultFunctionEncoder().encodeParameters(listOf(order, amounts, memo))

        val orderFilledTopic0 = org.web3j.crypto.Hash.sha3String(
            "OrderFilled(address,(uint256,(address,int256),bool),uint256[],string)",
        )

        val raw = RawLogRecord(
            network = "ethereum",
            contractAddress = "0xcontract",
            txHash = "0xtx",
            logIndex = 2,
            blockNumber = 500,
            blockHash = "0xblk",
            topics = listOf(orderFilledTopic0, addressTopic("0x00000000000000000000000000000000000000ff")),
            data = data,
            source = IngestionSource.POLL,
            observedAtEpochMillis = 1,
        )

        val result = decoder.decode(raw, "nested-orders")
        result.shouldBeInstanceOf<DecodeResult.Success>()
        val f = result.envelope.decodedFields

        f.getValue("maker").jsonPrimitive.content shouldBe "0x00000000000000000000000000000000000000ff"

        val orderObj = f.getValue("order").jsonObject
        orderObj.getValue("id").jsonPrimitive.let {
            it.isString shouldBe true
            it.content shouldBe "7"
        }
        orderObj.getValue("active").jsonPrimitive.content shouldBe "true"
        val assetObj = orderObj.getValue("asset").jsonObject
        assetObj.getValue("token").jsonPrimitive.content shouldBe "0x00000000000000000000000000000000000000ab"
        // negative int256 leaf, still a string
        assetObj.getValue("delta").jsonPrimitive.let {
            it.isString shouldBe true
            it.content shouldBe "-5"
        }

        val amountsArr = f.getValue("amounts").jsonArray
        amountsArr.map { it.jsonPrimitive.content } shouldBe listOf("1", "2", "10")
        amountsArr.forEach { it.jsonPrimitive.isString shouldBe true }

        f.getValue("memo").jsonPrimitive.content shouldBe "hello"
    }

    @Test
    fun `log with topic count mismatch is routed to dead letter with a clear reason`() {
        val raw = RawLogRecord(
            "ethereum", "0xaa", "0xtx", 0, 1, "0xb",
            topics = listOf(transferTopic0, addressTopic("0x01")), // Transfer needs 2 indexed, only 1 given
            data = word(BigInteger.ONE),
            source = IngestionSource.POLL,
            observedAtEpochMillis = 1,
        )

        val result = decoder.decode(raw, "erc20")
        result.shouldBeInstanceOf<DecodeResult.Failure>()
        result.record.abiRef shouldBe "erc20"
        result.record.rawLog shouldBe raw
        result.record.reason shouldContain "topic count mismatch"
    }

    @Test
    fun `unknown abiRef produces a dead letter record rather than throwing`() {
        val raw = RawLogRecord(
            "ethereum", "0xaa", "0xtx", 0, 1, "0xb",
            topics = listOf(transferTopic0, addressTopic("0x01"), addressTopic("0x02")),
            data = word(BigInteger.ONE),
            source = IngestionSource.POLL,
            observedAtEpochMillis = 1,
        )

        val result = decoder.decode(raw, "does-not-exist")
        result.shouldBeInstanceOf<DecodeResult.Failure>()
        result.record.reason shouldContain "unknown abiRef"
    }

    @Test
    fun `log whose topic0 matches no event in the abi is dead lettered`() {
        val raw = RawLogRecord(
            "ethereum", "0xaa", "0xtx", 0, 1, "0xb",
            topics = listOf("0x" + "ab".repeat(32), addressTopic("0x01")),
            data = "0x",
            source = IngestionSource.POLL,
            observedAtEpochMillis = 1,
        )

        val result = decoder.decode(raw, "erc20")
        result.shouldBeInstanceOf<DecodeResult.Failure>()
        result.record.reason shouldContain "no event"
    }
}
