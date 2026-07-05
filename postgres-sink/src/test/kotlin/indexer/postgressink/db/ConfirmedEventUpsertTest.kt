package indexer.postgressink.db

import indexer.postgressink.testsupport.EnvelopeFixtures
import indexer.schema.ConfirmationStatus
import indexer.schema.IngestionSource
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.Test

/**
 * Pure unit test (test pyramid layer 1, design doc section 5.2): no database,
 * no containers - proves the SQL text and parameter ordering that
 * ConfirmedEventRepository relies on to satisfy 4.7's upsert-on-
 * (network, tx_hash, log_index) requirement.
 */
class ConfirmedEventUpsertTest {

    @Test
    fun `upsert SQL declares the network, tx_hash, log_index conflict target`() {
        ConfirmedEventUpsert.SQL shouldContain "ON CONFLICT (network, tx_hash, log_index)"
    }

    @Test
    fun `upsert SQL updates status on conflict so INVALIDATED corrections overwrite CONFIRMED rows`() {
        ConfirmedEventUpsert.SQL shouldContain "status = EXCLUDED.status"
    }

    @Test
    fun `upsert SQL inserts into confirmed_events with a jsonb decoded_fields column`() {
        ConfirmedEventUpsert.SQL shouldContain "INSERT INTO confirmed_events"
        ConfirmedEventUpsert.SQL shouldContain "?::jsonb"
    }

    @Test
    fun `paramsFor orders values to match the SQL placeholders exactly`() {
        val envelope = EnvelopeFixtures.transfer(
            network = "ethereum",
            txHash = "0xabc123",
            logIndex = 7,
            blockNumber = 100L,
            status = ConfirmationStatus.CONFIRMED,
            source = IngestionSource.POLL,
        )

        val params = ConfirmedEventUpsert.paramsFor(envelope)

        params[0] shouldBe "ethereum"
        params[1] shouldBe "0xabc123"
        params[2] shouldBe 7L
        params[3] shouldBe "Transfer"
        params[4] shouldBe envelope.signatureHash
        params[5] shouldBe envelope.contractAddress
        params[6] shouldBe 100L
        params[7] shouldBe "CONFIRMED"
        params[8] shouldBe "POLL"
        params[9] as String shouldContain "\"from\":\"0xfrom\""
    }

    @Test
    fun `paramsFor never emits a raw numeric literal for on-chain integers in decodedFields`() {
        val nearMax = java.math.BigInteger.TWO.pow(256).subtract(java.math.BigInteger.ONE)
        val envelope = EnvelopeFixtures.transfer(value = nearMax)

        val decodedFieldsJson = ConfirmedEventUpsert.paramsFor(envelope)[9] as String

        decodedFieldsJson shouldContain "\"value\":\"$nearMax\""
    }
}
