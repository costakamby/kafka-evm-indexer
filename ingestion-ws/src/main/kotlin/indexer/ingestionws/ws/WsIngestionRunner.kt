package indexer.ingestionws.ws

import indexer.ingestionws.kafka.RawLogProducer
import indexer.ingestionws.rpc.EthRpcClient
import indexer.ingestionws.rpc.toRawLogRecord
import indexer.ingestionws.subscriptions.SubscriptionsReader
import indexer.schema.IngestionSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Per-network WS ingestion orchestrator (design decision 7 / acceptance
 * criterion 4.2 - "WS listener reconnects automatically on disconnect and
 * issues an eth_getLogs catch-up call for the exact gap range before
 * resubscribing"). Owns:
 *
 *  - fetching the currently active subscribed contract addresses for
 *    [network] from [subscriptionsReader], re-fetched on every (re)connect.
 *    Address-set changes therefore take effect on the next reconnect cycle -
 *    a documented scope limit (see module report): a healthy live
 *    connection is never torn down purely to pick up a new address sooner.
 *  - establishing a block-number baseline (`eth_blockNumber`) on the very
 *    first connect, so even a disconnect before any log ever arrives has an
 *    accurate starting point for a future catch-up.
 *  - on every subsequent reconnect, backing off and then issuing an exact
 *    `eth_getLogs` catch-up call [lastSeenBlock+1, currentHead] BEFORE
 *    resubscribing - never trusting WS to have replayed anything.
 *  - tagging every produced RawLogRecord source=WS and the correct
 *    [network], both for live notifications and catch-up logs.
 *  - never reading or using a subscription's `startBlock` - WS is
 *    live-only by construction; historical backfill from a past start_block
 *    is the poller module's job, not this one's.
 */
class WsIngestionRunner(
    private val network: String,
    private val transport: WsTransport,
    private val rpcClient: EthRpcClient,
    private val subscriptionsReader: SubscriptionsReader,
    private val producer: RawLogProducer,
    private val backoff: ExponentialBackoff = ExponentialBackoff(),
    private val subscriptionRefreshInterval: Duration = 5.seconds,
    private val clock: () -> Long = { System.currentTimeMillis() },
) {
    private val logger = LoggerFactory.getLogger(WsIngestionRunner::class.java)

    @Volatile
    var lastSeenBlock: Long? = null
        private set

    suspend fun run() {
        var attempt = 0
        while (currentCoroutineContext().isActive) {
            try {
                val addresses = subscriptionsReader.fetchSubscriptions(network = network).map { it.address }.distinct()
                if (addresses.isEmpty()) {
                    delay(subscriptionRefreshInterval)
                    continue
                }

                if (lastSeenBlock == null) {
                    lastSeenBlock = rpcClient.ethBlockNumber()
                } else if (attempt > 0) {
                    delay(backoff.delayFor(attempt))
                    performCatchUp(addresses)
                }

                transport.connectAndStream(addresses) { log ->
                    val record = log.toRawLogRecord(network, IngestionSource.WS, clock())
                    producer.send(record)
                    lastSeenBlock = maxOf(lastSeenBlock ?: -1L, record.blockNumber)
                }
                attempt = 0
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // Covers a transient failure ANYWHERE in this cycle - fetching active
                // subscriptions, establishing/refreshing the block-number baseline, the
                // catch-up eth_getLogs call, or the WS stream itself. All are equally
                // "this cycle didn't complete, retry" - none may propagate uncaught out
                // of run(), or they'd crash the whole process (a real outage this project
                // hit: a brief subscription-api restart killed every network's WS listener
                // in one shot, not just that cycle).
                logger.warn("[{}] WS ingestion cycle failed, will reconnect: {}", network, e.message)
            }
            attempt += 1
        }
    }

    /**
     * Fills the exact gap [lastSeenBlock+1, currentHead] via eth_getLogs -
     * see [CatchupRange] and design decision 7. Tags every recovered log
     * source=WS (see module report for why: this eth_getLogs call is made
     * by the WS listener itself, closing its own gap - it is not an
     * independent poll corroboration).
     */
    private suspend fun performCatchUp(addresses: List<String>) {
        val head = rpcClient.ethBlockNumber()
        val range = CatchupRange.compute(lastSeenBlock, head)
        if (range != null) {
            val logs = rpcClient.ethGetLogs(range.first, range.last, addresses)
            logs.forEach { log ->
                producer.send(log.toRawLogRecord(network, IngestionSource.WS, clock()))
            }
        }
        lastSeenBlock = head
    }
}
