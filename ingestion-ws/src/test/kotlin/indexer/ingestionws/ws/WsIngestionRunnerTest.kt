package indexer.ingestionws.ws

import indexer.ingestionws.kafka.RawLogProducer
import indexer.ingestionws.rpc.EthRpcClient
import indexer.ingestionws.rpc.RawLogDto
import indexer.ingestionws.subscriptions.SubscriptionStatusFilter
import indexer.ingestionws.subscriptions.SubscriptionsReader
import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord
import indexer.schema.SubscriptionRecord
import indexer.schema.SubscriptionStatus
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

/**
 * Orchestration-level test for [WsIngestionRunner] (pyramid layer close to
 * "TopologyTestDriver" tier per 5.2: fast, deterministic, no real sockets or
 * containers - every collaborator is a fake/spy). This is the primary proof
 * for acceptance criterion 4.2:
 *   "WS listener reconnects automatically on disconnect and issues an
 *    eth_getLogs catch-up call for the exact gap range before resubscribing.
 *    No log is lost across a simulated WS disconnect."
 * (scoped to raw-logs-topic production, per the task brief - the full
 * confirmed-output criterion needs the rest of the pipeline, not owned here)
 *
 * It also proves: every raw log (live or catch-up) is tagged source=WS and
 * the correct network, and that WS never reads/uses a subscription's
 * start_block anywhere (it is WS-live-only by construction) - a distinctive
 * sentinel startBlock value is used and asserted to never appear in any
 * eth_getLogs call.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class WsIngestionRunnerTest {

    private val sentinelStartBlock = 999_000_000L

    private class RecordingRawLogProducer : RawLogProducer {
        val sent = CopyOnWriteArrayList<RawLogRecord>()
        override fun send(record: RawLogRecord) {
            sent.add(record)
        }
    }

    private class FixedSubscriptionsReader(private val addresses: List<String>, private val startBlock: Long) : SubscriptionsReader {
        override suspend fun fetchSubscriptions(network: String?, status: SubscriptionStatusFilter): List<SubscriptionRecord> =
            addresses.map { address ->
                SubscriptionRecord(
                    id = "sub-$address",
                    network = network ?: "ethereum",
                    address = address,
                    abiRef = "erc20-v1",
                    startBlock = startBlock,
                    includeEvents = listOf("Transfer"),
                    status = SubscriptionStatus.ACTIVE,
                    createdAtEpochMillis = 0L,
                )
            }
    }

    /** Records every eth_getLogs / eth_blockNumber call so the test can assert exact gap ranges. */
    private class RecordingEthRpcClient(
        private val blockNumbers: MutableList<Long>,
        private val logsByRange: Map<Pair<Long, Long>, List<RawLogDto>>,
    ) : EthRpcClient {
        val getLogsCalls = CopyOnWriteArrayList<Triple<Long, Long, List<String>>>()

        override suspend fun ethBlockNumber(): Long = blockNumbers.removeAt(0)

        override suspend fun ethGetLogs(fromBlock: Long, toBlock: Long, addresses: List<String>): List<RawLogDto> {
            getLogsCalls.add(Triple(fromBlock, toBlock, addresses))
            return logsByRange[fromBlock to toBlock] ?: emptyList()
        }
    }

    private fun logDto(block: Long, logIndex: Long, txHash: String) = RawLogDto(
        address = "0xabc",
        blockHash = "0xblock$block",
        blockNumber = "0x" + block.toString(16),
        data = "0xdata",
        logIndex = "0x" + logIndex.toString(16),
        topics = listOf("0xtopic0"),
        transactionHash = txHash,
    )

    @Test
    fun `reconnect issues an exact-range catch-up call before resubscribing, with zero gap and correct tagging`() = runTest {
        // First connect establishes a baseline at head=100. After the first
        // stream drops (simulated disconnect), the chain has advanced to head=103
        // and mined one log at block 101 *while WS was down* - this is exactly the
        // gap eth_getLogs must fill before resubscribing.
        val rpcClient = RecordingEthRpcClient(
            blockNumbers = mutableListOf(100L, 103L),
            logsByRange = mapOf((101L to 103L) to listOf(logDto(block = 101, logIndex = 0, txHash = "0xtx-catchup"))),
        )
        val producer = RecordingRawLogProducer()
        val subscriptionsReader = FixedSubscriptionsReader(addresses = listOf("0xabc"), startBlock = sentinelStartBlock)

        val connectCount = AtomicInteger(0)
        val secondStreamStarted = CompletableDeferred<Unit>()
        val transport = object : WsTransport {
            override suspend fun connectAndStream(addresses: List<String>, onLog: suspend (RawLogDto) -> Unit) {
                when (connectCount.incrementAndGet()) {
                    1 -> {
                        // live notification arrives, then the connection drops (returns normally = disconnect)
                        onLog(logDto(block = 100, logIndex = 0, txHash = "0xtx-live-1"))
                    }
                    else -> {
                        onLog(logDto(block = 104, logIndex = 0, txHash = "0xtx-live-2"))
                        secondStreamStarted.complete(Unit)
                        // stay "connected" (suspend) until the test cancels the runner
                        awaitCancellation()
                    }
                }
            }
        }

        val runner = WsIngestionRunner(
            network = "ethereum",
            transport = transport,
            rpcClient = rpcClient,
            subscriptionsReader = subscriptionsReader,
            producer = producer,
            backoff = ExponentialBackoff(initial = 1.milliseconds, max = 5.milliseconds, multiplier = 1.0),
            clock = { 0L },
        )

        val job = launch { runner.run() }
        advanceUntilIdle()
        secondStreamStarted.await()
        advanceUntilIdle()
        job.cancel()

        // Zero gap, correct order, all tagged WS + network=ethereum.
        val blocks = producer.sent.map { it.blockNumber }
        blocks shouldBe listOf(100L, 101L, 104L)
        producer.sent.forEach {
            it.source shouldBe IngestionSource.WS
            it.network shouldBe "ethereum"
        }
        producer.sent.first { it.blockNumber == 101L }.txHash shouldBe "0xtx-catchup"

        // The catch-up call used the exact gap range [lastSeenBlock+1, head], nothing else.
        rpcClient.getLogsCalls shouldBe listOf(Triple(101L, 103L, listOf("0xabc")))

        // WS never touched the subscription's start_block anywhere.
        rpcClient.getLogsCalls.forEach { (from, to, _) ->
            (from == sentinelStartBlock || to == sentinelStartBlock) shouldBe false
        }
    }

    @Test
    fun `never calls eth_getLogs when there is nothing to catch up on the very first connect`() = runTest {
        val rpcClient = RecordingEthRpcClient(blockNumbers = mutableListOf(50L), logsByRange = emptyMap())
        val producer = RecordingRawLogProducer()
        val subscriptionsReader = FixedSubscriptionsReader(addresses = listOf("0xabc"), startBlock = sentinelStartBlock)
        val started = CompletableDeferred<Unit>()

        val transport = object : WsTransport {
            override suspend fun connectAndStream(addresses: List<String>, onLog: suspend (RawLogDto) -> Unit) {
                started.complete(Unit)
                awaitCancellation()
            }
        }

        val runner = WsIngestionRunner(
            network = "ethereum",
            transport = transport,
            rpcClient = rpcClient,
            subscriptionsReader = subscriptionsReader,
            producer = producer,
        )

        val job = launch { runner.run() }
        started.await()
        advanceUntilIdle()
        job.cancel()

        rpcClient.getLogsCalls shouldBe emptyList()
    }
}
