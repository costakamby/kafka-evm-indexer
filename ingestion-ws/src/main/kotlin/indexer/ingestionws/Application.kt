package indexer.ingestionws

import indexer.ingestionws.config.AppConfigLoader
import indexer.ingestionws.kafka.KafkaRawLogProducer
import indexer.ingestionws.rpc.EthRpcHttpClient
import indexer.ingestionws.subscriptions.HttpSubscriptionsReader
import indexer.ingestionws.ws.KtorWsTransport
import indexer.ingestionws.ws.WsIngestionRunner
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.websocket.WebSockets
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties
import kotlin.time.Duration.Companion.seconds

private val logger = LoggerFactory.getLogger("indexer.ingestionws.Application")

/**
 * Entry point: one [WsIngestionRunner] per configured network, all sharing
 * one ktor HttpClient and one KafkaProducer. Every WS listener runs in a
 * `supervisorScope` so one network's WS listener crashing (after exhausting
 * its own reconnect loop's error handling - which should never actually
 * happen, since [WsIngestionRunner.run] loops forever) never brings down the
 * others.
 *
 * NOTE (see report): every `wsRpcUrl` in application.yaml is a placeholder -
 * a real `wss://` endpoint (Alchemy/Infura or similar) must be supplied via
 * config/env override before this connects to anything real.
 */
fun main() {
    val config = AppConfigLoader.load()
    logger.info("Starting ingestion-ws for networks: {}", config.networks.keys)

    val httpClient = HttpClient(CIO) { install(WebSockets) }
    val producerProps = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    }
    val kafkaProducer = KafkaProducer<String, String>(producerProps)
    val rawLogProducer = KafkaRawLogProducer(kafkaProducer, topic = config.kafka.rawLogsTopic)
    val subscriptionsReader = HttpSubscriptionsReader(httpClient, config.subscriptionApi.baseUrl)

    runBlocking {
        supervisorScope {
            config.networks.forEach { (network, networkConfig) ->
                val runner = WsIngestionRunner(
                    network = network,
                    transport = KtorWsTransport(httpClient, networkConfig.wsRpcUrl),
                    rpcClient = EthRpcHttpClient(httpClient, networkConfig.rpcUrl),
                    subscriptionsReader = subscriptionsReader,
                    producer = rawLogProducer,
                    subscriptionRefreshInterval = config.subscriptionApi.refreshIntervalSeconds.seconds,
                )
                launch {
                    logger.info("[{}] starting WS ingestion (ws={})", network, networkConfig.wsRpcUrl)
                    runner.run()
                }
            }
        }
    }
}
