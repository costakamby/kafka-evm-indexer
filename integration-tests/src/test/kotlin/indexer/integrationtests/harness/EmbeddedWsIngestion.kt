package indexer.integrationtests.harness

import indexer.ingestionws.kafka.KafkaRawLogProducer
import indexer.ingestionws.rpc.EthRpcHttpClient
import indexer.ingestionws.subscriptions.HttpSubscriptionsReader
import indexer.ingestionws.ws.KtorWsTransport
import indexer.ingestionws.ws.WsIngestionRunner
import indexer.streamstopology.Topics
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.websocket.WebSockets
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import kotlin.time.Duration.Companion.seconds

/**
 * A real ingestion-ws instance: the production [WsIngestionRunner] pointed at
 * Anvil's own `ws://` endpoint (a genuine eth_subscribe(logs) target, per the
 * build brief) and Anvil's HTTP endpoint for baseline/catch-up calls, emitting
 * source=WS raw logs to raw-logs-topic. This is the live path that re-observes a
 * block height when a reorg replaces it - the watermark-based poller never
 * re-fetches an already-polled height, so WS is what makes the reorg's
 * replacement log reach the pipeline.
 */
class EmbeddedWsIngestion(
    private val bootstrapServers: String,
    private val anvilWsUrl: String,
    private val anvilHttpRpcUrl: String,
    private val subscriptionApiBaseUrl: String,
    private val network: String,
) : AutoCloseable {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var job: Job? = null

    private val httpClient = HttpClient(CIO) { install(WebSockets) }
    private val producer = KafkaProducer<String, String>(
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.ACKS_CONFIG, "all")
        },
        StringSerializer(),
        StringSerializer(),
    )
    private val rawLogProducer = KafkaRawLogProducer(producer, Topics.RAW_LOGS)
    private val subscriptionsReader = HttpSubscriptionsReader(httpClient, subscriptionApiBaseUrl)

    private val runner = WsIngestionRunner(
        network = network,
        transport = KtorWsTransport(httpClient, anvilWsUrl),
        rpcClient = EthRpcHttpClient(httpClient, anvilHttpRpcUrl),
        subscriptionsReader = subscriptionsReader,
        producer = rawLogProducer,
        subscriptionRefreshInterval = 2.seconds,
    )

    fun start() {
        job = scope.launch { runner.run() }
    }

    override fun close() {
        scope.cancel()
        try {
            httpClient.close()
        } catch (_: Exception) {
        }
        try {
            producer.close()
        } catch (_: Exception) {
        }
    }
}
