package indexer.schema

import indexer.schema.json.IndexerJson
import indexer.schema.json.bigIntegerJsonField
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test
import java.math.BigInteger

/**
 * Every decoded-event message is a fixed envelope plus an ABI-dependent
 * decodedFields blob (design doc section 1/2). This proves the envelope
 * round-trips through the shared JSON config, and that a BigInteger value
 * placed into decodedFields is never a raw JSON number literal.
 */
class DecodedEventEnvelopeTest {

    @Test
    fun `envelope round trips through the shared Json config`() {
        val nearMax = BigInteger.TWO.pow(256).subtract(BigInteger.ONE)
        val envelope = DecodedEventEnvelope(
            eventName = "Transfer",
            signatureHash = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            network = "ethereum",
            contractAddress = "0x0000000000000000000000000000000000000001",
            txHash = "0xabc",
            logIndex = 3,
            blockNumber = 100L,
            status = ConfirmationStatus.UNCONFIRMED,
            source = IngestionSource.POLL,
            decodedFields = JsonObject(
                mapOf(
                    "from" to JsonPrimitive("0xfrom"),
                    "to" to JsonPrimitive("0xto"),
                    "value" to bigIntegerJsonField(nearMax),
                ),
            ),
        )

        val encoded = IndexerJson.instance.encodeToString(DecodedEventEnvelope.serializer(), envelope)
        val decoded = IndexerJson.instance.decodeFromString(DecodedEventEnvelope.serializer(), encoded)

        decoded shouldBe envelope
        // the "value" field must be a JSON string, never a raw numeric literal
        val valueElement = decoded.decodedFields.getValue("value").jsonPrimitive
        valueElement.isString shouldBe true
        valueElement.content shouldBe nearMax.toString()
    }

    @Test
    fun `EventKey builds a stable composite key for reconciliation and confirmation state`() {
        val key = EventKey.of(network = "ethereum", txHash = "0xabc", logIndex = 3)

        key shouldBe "ethereum:0xabc:3"
    }
}
