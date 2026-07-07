package indexer.integrationtests

import indexer.integrationtests.fixtures.Erc20Fixture
import indexer.integrationtests.harness.FullPipeline
import indexer.integrationtests.harness.KafkaRecordTail
import indexer.integrationtests.harness.KafkaRecordTail.Companion.confirmedEventsTail
import indexer.integrationtests.harness.KafkaRecordTail.Companion.rawLogsTail
import indexer.integrationtests.harness.KafkaTestBroker
import indexer.integrationtests.harness.SharedInfra
import indexer.integrationtests.harness.TEST_NETWORK
import indexer.integrationtests.harness.eventsFor
import indexer.integrationtests.harness.hasWsLogFor
import indexer.integrationtests.harness.historyFor
import indexer.schema.ConfirmationStatus
import indexer.schema.DecodedEventEnvelope
import indexer.schema.EventKey
import indexer.schema.RawLogRecord
import org.awaitility.core.ConditionTimeoutException
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.web3j.protocol.core.methods.response.TransactionReceipt
import java.math.BigInteger
import java.time.Duration
import javax.sql.DataSource

/**
 * The 4.5 acceptance bar: a real event, produced on an Anvil fork, that is still
 * UNCONFIRMED when a reorg removes it must end up INVALIDATED - both on
 * confirmed-events-topic (an explicit correction, never a silent contradiction)
 * and, corrected in place, in the Postgres read-model (4.7). The replacement
 * transaction at the same height must decode and progress normally.
 *
 * Full stack in one JVM: subscription-api (decode + reconciliation +
 * block-tracking + confirmation) + ingestion-poll + ingestion-ws + postgres-sink.
 * ingestion-ws is essential here: its live eth_subscribe re-observes the reorged
 * height with a new block hash, which the watermark-based poller never re-fetches.
 */
class ReorgEndToEndTest {

    companion object {
        private lateinit var kafka: KafkaTestBroker
        private lateinit var dataSource: DataSource
        private lateinit var pipeline: FullPipeline
        private lateinit var tail: KafkaRecordTail<DecodedEventEnvelope>
        private lateinit var rawTail: KafkaRecordTail<RawLogRecord>

        private const val RECIPIENT = "0x00000000000000000000000000000000000000f1"
        private val log = LoggerFactory.getLogger("harness.ReorgTest")

        @JvmStatic
        @BeforeAll
        fun setUp() {
            kafka = KafkaTestBroker().start()
            dataSource = SharedInfra.newPostgresDataSource()
            pipeline = FullPipeline(kafka.bootstrapServers, SharedInfra.anvil, dataSource, TEST_NETWORK, includeWs = true)
            pipeline.start()
            tail = confirmedEventsTail(kafka.bootstrapServers).start()
            rawTail = rawLogsTail(kafka.bootstrapServers).start()
        }

        @JvmStatic
        @AfterAll
        fun tearDown() {
            if (::rawTail.isInitialized) rawTail.close()
            if (::tail.isInitialized) tail.close()
            if (::pipeline.isInitialized) pipeline.close()
            if (::kafka.isInitialized) kafka.close()
        }
    }

    private val anvil get() = SharedInfra.anvil
    private var rawLogHistoryContractAddress: String? = null

    private fun logIndex(receipt: TransactionReceipt): Long = receipt.logs.first().logIndex.toLong()

    /**
     * Awaitility-backed wait on real pipeline state (never Thread.sleep, per design
     * doc 5.4) that enriches a timeout with a full pipeline-state dump - the
     * cross-task block-tracking/confirmation bug this test caught (see
     * NetworkStreamPartitioner's kdoc) was only diagnosable with exactly this kind
     * of dump, so it is kept as a standing diagnostic aid, not a one-off.
     */
    private fun awaitOrDump(what: String, timeout: Duration, keyA: String, keyB: String, condition: () -> Boolean) {
        try {
            await.atMost(timeout).pollInterval(Duration.ofMillis(300)).until(condition)
        } catch (e: ConditionTimeoutException) {
            val dump = buildString {
                append("TIMED OUT waiting for: ").append(what).append('\n')
                append("keyA store=").append(pipeline.api.confirmationState(keyA)?.status)
                append(" keyB store=").append(pipeline.api.confirmationState(keyB)?.status).append('\n')
                append("blockTracking=").append(pipeline.api.blockTrackingState(TEST_NETWORK)).append('\n')
                append("raw-logs-topic arrival order for this contract:\n")
                rawLogHistoryContractAddress?.let { addr ->
                    rawTail.historyFor(addr).forEach {
                        append("  source=").append(it.source)
                        append(" block=").append(it.blockNumber)
                        append(" hash=").append(it.blockHash)
                        append(" tx=").append(it.txHash).append('\n')
                    }
                }
                append("confirmed-events-topic emissions:\n")
                tail.all().forEach { append("  ").append(it.txHash).append(" logIdx=").append(it.logIndex).append(" status=").append(it.status).append('\n') }
            }
            throw AssertionError(dump, e)
        }
    }

    @Test
    fun `an event reorged out before confirmation is marked INVALIDATED end to end`() {
        val erc20 = Erc20Fixture(anvil.web3j, anvil.credentials, anvil.chainId)
        erc20.deploy(BigInteger.valueOf(1_000_000))
        rawLogHistoryContractAddress = erc20.contractAddress
        val startBlock = anvil.web3j.ethBlockNumber().send().blockNumber.toLong()

        val post = pipeline.rest.createSubscription(TEST_NETWORK, erc20.contractAddress, "erc20", startBlock, listOf("Transfer"))
        assertEquals(201, post.statusCode, "POST /subscriptions -> ${post.body}")
        awaitOrDump("subscription active", Duration.ofSeconds(20), "", "") {
            pipeline.rest.listActive(TEST_NETWORK).any { it.address.equals(erc20.contractAddress, ignoreCase = true) }
        }

        // Warm up ingestion-ws until its LIVE eth_subscribe is demonstrably delivering
        // source=WS raw logs for this contract (auto-mine is instant, so a log minted
        // before the WS handshake completes is never pushed; and only WS - never the
        // watermark-based poller - re-observes the reorged height). We keep minting
        // transfers until a WS-sourced raw log for this contract lands on raw-logs-topic.
        awaitOrDump("ingestion-ws delivering source=WS logs", Duration.ofSeconds(90), "", "") {
            erc20.transfer(RECIPIENT, BigInteger.valueOf(9))
            rawTail.hasWsLogFor(erc20.contractAddress)
        }

        // Snapshot BEFORE txA, mine txA, and gate on it being UNCONFIRMED in the real
        // confirmation store (deterministic, not a sleep) so the reorg has something to invalidate.
        val snapshotId = anvil.rpc.snapshot()
        val originalReceipt = erc20.transfer(RECIPIENT, BigInteger.valueOf(111))
        val txA = originalReceipt.transactionHash
        val logIndexA = logIndex(originalReceipt)
        val keyA = EventKey.of(TEST_NETWORK, txA, logIndexA)

        awaitOrDump("txA UNCONFIRMED", Duration.ofSeconds(60), keyA, "") {
            pipeline.api.confirmationState(keyA)?.status == ConfirmationStatus.UNCONFIRMED
        }
        assertEquals(ConfirmationStatus.UNCONFIRMED, pipeline.api.confirmationState(keyA)?.status)

        // Reorg it out: revert and mine an alternate block at the same height (txB).
        check(anvil.rpc.revert(snapshotId)) { "evm_revert failed" }
        val replacementReceipt = erc20.transfer(RECIPIENT, BigInteger.valueOf(222))
        val txB = replacementReceipt.transactionHash
        val logIndexB = logIndex(replacementReceipt)
        val keyB = EventKey.of(TEST_NETWORK, txB, logIndexB)
        log.info(
            "reorg: txA={} block={} hash={} | txB={} block={} hash={}",
            txA, originalReceipt.blockNumber, originalReceipt.blockHash,
            txB, replacementReceipt.blockNumber, replacementReceipt.blockHash,
        )

        // Push txB past the depth threshold with further transfers at higher heights.
        repeat(4) { erc20.transfer(RECIPIENT, BigInteger.valueOf(3)) }

        // (a) txA ends INVALIDATED in the confirmation store...
        awaitOrDump("txA INVALIDATED in store", Duration.ofSeconds(90), keyA, keyB) {
            pipeline.api.confirmationState(keyA)?.status == ConfirmationStatus.INVALIDATED
        }
        // ...and an explicit INVALIDATED correction was emitted on confirmed-events-topic (4.5).
        awaitOrDump("txA INVALIDATED on confirmed-events-topic", Duration.ofSeconds(30), keyA, keyB) {
            tail.eventsFor(txA, logIndexA).any { it.status == ConfirmationStatus.INVALIDATED }
        }
        // txA was never optimistically CONFIRMED (still UNCONFIRMED at reorg time) - only INVALIDATED.
        assertTrue(
            tail.eventsFor(txA, logIndexA).none { it.status == ConfirmationStatus.CONFIRMED },
            "txA must never appear as CONFIRMED: ${tail.eventsFor(txA, logIndexA).map { it.status }}",
        )

        // (b) Postgres reflects the correction in place: one row, INVALIDATED (4.7).
        awaitOrDump("txA INVALIDATED in Postgres", Duration.ofSeconds(60), keyA, keyB) {
            pipeline.repository.findByKey(TEST_NETWORK, txA, logIndexA)?.status == ConfirmationStatus.INVALIDATED
        }
        assertEquals(1L, pipeline.repository.countForKey(TEST_NETWORK, txA, logIndexA), "correction must update in place, not duplicate")

        // (c) the replacement txB decodes and progresses normally to CONFIRMED.
        awaitOrDump("txB CONFIRMED in Postgres", Duration.ofSeconds(90), keyA, keyB) {
            pipeline.repository.findByKey(TEST_NETWORK, txB, logIndexB)?.status == ConfirmationStatus.CONFIRMED
        }
        assertTrue(txA != txB)
    }
}
