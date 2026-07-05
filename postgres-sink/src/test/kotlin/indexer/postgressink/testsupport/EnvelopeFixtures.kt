package indexer.postgressink.testsupport

import indexer.schema.ConfirmationStatus
import indexer.schema.DecodedEventEnvelope
import indexer.schema.IngestionSource
import indexer.schema.json.bigIntegerJsonField
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.math.BigInteger

/** Small, deterministic DecodedEventEnvelope builder shared across postgres-sink tests. */
object EnvelopeFixtures {

    fun transfer(
        network: String = "ethereum",
        txHash: String = "0xabc123",
        logIndex: Long = 0,
        blockNumber: Long = 100L,
        status: ConfirmationStatus = ConfirmationStatus.CONFIRMED,
        source: IngestionSource = IngestionSource.POLL,
        value: BigInteger = BigInteger.TWO.pow(256).subtract(BigInteger.ONE),
    ): DecodedEventEnvelope = DecodedEventEnvelope(
        eventName = "Transfer",
        signatureHash = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        network = network,
        contractAddress = "0x0000000000000000000000000000000000000001",
        txHash = txHash,
        logIndex = logIndex,
        blockNumber = blockNumber,
        status = status,
        source = source,
        decodedFields = JsonObject(
            mapOf(
                "from" to JsonPrimitive("0xfrom"),
                "to" to JsonPrimitive("0xto"),
                "value" to bigIntegerJsonField(value),
            ),
        ),
    )
}
