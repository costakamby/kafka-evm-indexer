package indexer.integrationtests.harness

import indexer.ingestionpoll.poll.ContractPoller
import indexer.ingestionpoll.progress.KafkaPollProgressStore
import indexer.ingestionpoll.rpc.HttpEthRpcClient
import indexer.ingestionpoll.subscriptions.HttpSubscriptionsReader
import indexer.streamstopology.Topics
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * A real ingestion-poll instance: the production [ContractPoller] driven in a
 * loop exactly as ingestion-poll's `main()` drives it, reading active
 * subscriptions from the embedded subscription-api's REST endpoint and polling
 * Anvil's HTTP JSON-RPC (`eth_getLogs`) for one [network], emitting source=POLL
 * raw logs to raw-logs-topic. Uses a short poll interval so backfill lands fast
 * under test.
 */
class EmbeddedPoller(
    private val bootstrapServers: String,
    private val anvilHttpRpcUrl: String,
    private val subscriptionApiBaseUrl: String,
    private val network: String,
    private val pollIntervalMs: Long = 500,
) : AutoCloseable {

    private val log = LoggerFactory.getLogger("harness.EmbeddedPoller")
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var job: Job? = null

    private val producer = KafkaProducer<String, String>(
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.ACKS_CONFIG, "all")
        },
        StringSerializer(),
        StringSerializer(),
    )
    private val httpClient = HttpClient(CIO)
    private val subscriptionsReader = HttpSubscriptionsReader(httpClient, subscriptionApiBaseUrl)
    private val poller = ContractPoller(
        rpcClient = HttpEthRpcClient(HttpClient(CIO), anvilHttpRpcUrl, maxRetries = 5, initialBackoffMs = 100, maxBackoffMs = 2_000),
        progressStore = KafkaPollProgressStore(producer),
        producer = producer,
        rawLogsTopic = Topics.RAW_LOGS,
        maxBlockRange = 2_000,
    )

    fun start() {
        job = scope.launch {
            while (isActive) {
                try {
                    val subscriptions = subscriptionsReader.activeSubscriptions(network)
                    poller.pollNetwork(network, subscriptions)
                } catch (e: Exception) {
                    log.debug("poll cycle failed (will retry): {}", e.message)
                }
                delay(pollIntervalMs)
            }
        }
    }

    override fun close() {
        scope.cancel()
        try {
            producer.close()
        } catch (_: Exception) {
        }
        try {
            httpClient.close()
        } catch (_: Exception) {
        }
    }
}
