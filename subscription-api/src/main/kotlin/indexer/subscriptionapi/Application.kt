package indexer.subscriptionapi

import indexer.decoder.AbiRegistry
import indexer.decoder.EventDecoder
import indexer.schema.SubscriptionRecord
import indexer.streamstopology.IndexerTopology
import indexer.streamstopology.Topics
import indexer.streamstopology.config.NetworkParams
import indexer.streamstopology.config.NetworkTopologyConfig
import indexer.streamstopology.serde.jsonSerdeOf
import indexer.subscriptionapi.config.AppConfig
import indexer.subscriptionapi.config.AppConfigLoader
import indexer.subscriptionapi.subscriptions.AppDependencies
import indexer.subscriptionapi.subscriptions.KafkaStreamsSubscriptionReader
import indexer.subscriptionapi.subscriptions.KafkaSubscriptionWriter
import indexer.subscriptionapi.subscriptions.subscriptionModule
import indexer.topicadmin.TopicSetup
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.slf4j.LoggerFactory
import java.util.Properties

private val log = LoggerFactory.getLogger("indexer.subscriptionapi.Application")

fun main() {
    val config = AppConfigLoader.load()

    // Topics must exist before KafkaStreams starts (docker-compose Kafka has
    // auto.create.topics.enable=false).
    Admin.create(adminProps(config)).use { admin -> TopicSetup.ensureTopics(admin) }

    val decoder = EventDecoder(AbiRegistry())
    val networkTopologyConfig = NetworkTopologyConfig(
        config.networks.mapValues { (_, net) -> NetworkParams(confirmationDepth = net.confirmationDepth) },
    )
    val topology = IndexerTopology.build(networkTopologyConfig, decoder)
    val streams = KafkaStreams(topology, streamsProps(config))
    streams.start()

    val producer = KafkaProducer(
        producerProps(config),
        StringSerializer(),
        jsonSerdeOf(SubscriptionRecord.serializer()).serializer(),
    )

    val deps = AppDependencies(
        subscriptionWriter = KafkaSubscriptionWriter(producer, Topics.SUBSCRIPTIONS),
        subscriptionReader = KafkaStreamsSubscriptionReader(streams),
        abiRegistry = AbiRegistry(),
    )

    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("shutting down: closing KafkaStreams and producer")
            streams.close()
            producer.close()
        },
    )

    embeddedServer(Netty, port = config.server.port) {
        module()
        subscriptionModule(deps)
    }.start(wait = true)
}

private fun adminProps(config: AppConfig): Properties = Properties().apply {
    put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
}

private fun producerProps(config: AppConfig): Properties = Properties().apply {
    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
    put(ProducerConfig.ACKS_CONFIG, "all")
}

private fun streamsProps(config: AppConfig): Properties = Properties().apply {
    put(StreamsConfig.APPLICATION_ID_CONFIG, config.kafka.applicationId)
    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
    put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1)
}

fun Application.module() {
    val prometheusRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    JvmMemoryMetrics().bindTo(prometheusRegistry)
    JvmGcMetrics().bindTo(prometheusRegistry)
    JvmThreadMetrics().bindTo(prometheusRegistry)
    ProcessorMetrics().bindTo(prometheusRegistry)

    install(MicrometerMetrics) {
        registry = prometheusRegistry
    }
    install(ContentNegotiation) {
        json(Json { ignoreUnknownKeys = true })
    }
    install(CallLogging)

    routing {
        get("/health") {
            call.respond(mapOf("status" to "ok"))
        }
        get("/metrics") {
            call.respond(prometheusRegistry.scrape())
        }
    }
}
