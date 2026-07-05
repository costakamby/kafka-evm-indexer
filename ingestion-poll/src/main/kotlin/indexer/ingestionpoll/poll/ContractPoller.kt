package indexer.ingestionpoll.poll

import indexer.ingestionpoll.progress.PollProgressStore
import indexer.ingestionpoll.rpc.EthRpcClient
import indexer.schema.EventKey
import indexer.schema.SubscriptionRecord
import indexer.schema.json.IndexerJson
import kotlinx.serialization.encodeToString
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

/**
 * One network's poll cycle: for every ACTIVE subscribed contract, resolves
 * where to resume from (its own durable watermark, or its subscription's
 * start_block for a brand-new historical backfill, or the head if neither is
 * known), chunks the span into maxBlockRange-sized eth_getLogs calls, and
 * emits every log to raw-logs-topic tagged source=POLL (design doc 4.2).
 *
 * Progress is recorded after each chunk succeeds (not only at the end of the
 * whole span), so a crash mid-backfill resumes from the last completed chunk
 * boundary rather than restarting the entire backfill.
 */
class ContractPoller(
    private val rpcClient: EthRpcClient,
    private val progressStore: PollProgressStore,
    private val producer: Producer<String, String>,
    private val rawLogsTopic: String,
    private val maxBlockRange: Long,
) {
    private val log = LoggerFactory.getLogger(ContractPoller::class.java)

    suspend fun pollNetwork(network: String, subscriptions: List<SubscriptionRecord>) {
        if (subscriptions.isEmpty()) return
        val head = rpcClient.blockNumber()
        for (subscription in subscriptions) {
            pollContract(network, subscription, head)
        }
    }

    private suspend fun pollContract(network: String, subscription: SubscriptionRecord, head: Long) {
        val lastPolled = progressStore.lastPolledBlock(network, subscription.address)
        val fromBlock = BlockRangePlanner.resolveStartBlock(lastPolled, subscription.startBlock, head)
        if (fromBlock > head) {
            log.debug("nothing new for {}/{}: fromBlock {} is past head {}", network, subscription.address, fromBlock, head)
            return
        }

        val ranges = BlockRangePlanner.plan(fromBlock, head, maxBlockRange)
        for (range in ranges) {
            val logs = rpcClient.getLogs(range.fromBlock, range.toBlock, listOf(subscription.address))
            for (rawLog in logs) {
                val record = RawLogMapper.toRawLogRecord(network, rawLog)
                val key = EventKey.of(network, record.txHash, record.logIndex)
                producer.send(ProducerRecord(rawLogsTopic, key, IndexerJson.instance.encodeToString(record)))
            }
            progressStore.recordProgress(network, subscription.address, range.toBlock)
        }
    }
}
