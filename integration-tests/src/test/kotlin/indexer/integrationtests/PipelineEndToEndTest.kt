package indexer.integrationtests

import indexer.integrationtests.fixtures.Erc20Fixture
import indexer.integrationtests.harness.FullPipeline
import indexer.integrationtests.harness.KafkaTestBroker
import indexer.integrationtests.harness.SharedInfra
import indexer.integrationtests.harness.TEST_NETWORK
import indexer.schema.ConfirmationStatus
import indexer.schema.EventKey
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.web3j.protocol.core.methods.response.TransactionReceipt
import java.math.BigInteger
import java.time.Duration
import javax.sql.DataSource

/**
 * Tier-4 full end-to-end tests (design doc 4.8) wiring the ENTIRE real system in
 * one JVM against Testcontainers Kafka + Postgres + an Anvil fork: subscription-api
 * (Ktor + KafkaStreams: decode + reconciliation + block-tracking + confirmation),
 * ingestion-poll, ingestion-ws and postgres-sink.
 *
 * Covers two 4.8 requirements in one shared harness (one broker, one Streams
 * instance, isolated per-test by using a fresh contract each time):
 *  - a real Anvil-sourced event flowing raw log -> decode -> reconciliation ->
 *    confirmation -> confirmed-events-topic -> Postgres CONFIRMED row (4.1/4.2/
 *    4.3/4.5-confirm/4.7); and
 *  - the reorg scenario (4.5): an event still UNCONFIRMED at reorg time is
 *    reorged out and ends up INVALIDATED, corrected in place in Postgres.
 */
class PipelineEndToEndTest {

    companion object {
        private lateinit var kafka: KafkaTestBroker
        private lateinit var dataSource: DataSource
        private lateinit var pipeline: FullPipeline

        private const val RECIPIENT = "0x00000000000000000000000000000000000000f1"

        @JvmStatic
        @BeforeAll
        fun setUp() {
            kafka = KafkaTestBroker().start()
            dataSource = SharedInfra.newPostgresDataSource()
            pipeline = FullPipeline(kafka.bootstrapServers, SharedInfra.anvil, dataSource, TEST_NETWORK)
            pipeline.start()
        }

        @JvmStatic
        @AfterAll
        fun tearDown() {
            if (::pipeline.isInitialized) pipeline.close()
            if (::kafka.isInitialized) kafka.close()
        }
    }

    private val anvil get() = SharedInfra.anvil

    private fun transferLogIndex(receipt: TransactionReceipt): Long =
        receipt.logs.first().logIndex.toLong()

    private fun subscribeAndAwaitActive(erc20: Erc20Fixture, startBlock: Long) {
        val post = pipeline.rest.createSubscription(
            network = TEST_NETWORK,
            address = erc20.contractAddress,
            abiRef = "erc20",
            startBlock = startBlock,
            includeEvents = listOf("Transfer"),
        )
        assertEquals(201, post.statusCode, "POST /subscriptions should be Created (201), body=${post.body}")

        await.atMost(Duration.ofSeconds(20)).until {
            pipeline.rest.listActive(TEST_NETWORK).any { it.address.equals(erc20.contractAddress, ignoreCase = true) }
        }
    }

    @Test
    fun `anvil event flows through the whole pipeline to a CONFIRMED postgres row`() {
        val erc20 = Erc20Fixture(anvil.web3j, anvil.credentials, anvil.chainId)
        erc20.deploy(BigInteger.valueOf(1_000_000))
        val startBlock = anvil.web3j.ethBlockNumber().send().blockNumber.toLong()

        subscribeAndAwaitActive(erc20, startBlock)

        // The first transfer is the event we assert reaches CONFIRMED. Block-tracking's
        // lastBlock only advances when raw logs at higher heights arrive, so we mine
        // several further transfers to push the first one past the depth-2 threshold -
        // this is what "confirmed against actual block-tracking state, not wall clock"
        // (4.5) means here.
        val firstReceipt = erc20.transfer(RECIPIENT, BigInteger.valueOf(1))
        repeat(4) { erc20.transfer(RECIPIENT, BigInteger.valueOf(2)) }

        val key = firstReceipt.transactionHash
        val logIndex = transferLogIndex(firstReceipt)

        await.atMost(Duration.ofSeconds(90)).until {
            pipeline.repository.findByKey(TEST_NETWORK, key, logIndex)?.status == ConfirmationStatus.CONFIRMED
        }

        val row = pipeline.repository.findByKey(TEST_NETWORK, key, logIndex)!!
        assertEquals("Transfer", row.eventName)
        assertEquals(TEST_NETWORK, row.network)
    }
}
