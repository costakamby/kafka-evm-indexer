package indexer.integrationtests.harness

import indexer.decoder.AbiRegistry
import indexer.decoder.EventDecoder
import indexer.schema.SubscriptionRecord
import indexer.schema.ktable.BlockTrackingState
import indexer.schema.ktable.ConfirmationState
import indexer.streamstopology.IndexerTopology
import indexer.streamstopology.Topics
import indexer.streamstopology.config.NetworkTopologyConfig
import indexer.streamstopology.lifecycle.StoreNames
import indexer.streamstopology.serde.jsonSerdeOf
import indexer.subscriptionapi.module
import indexer.subscriptionapi.subscriptions.AppDependencies
import indexer.subscriptionapi.subscriptions.KafkaStreamsSubscriptionReader
import indexer.subscriptionapi.subscriptions.KafkaSubscriptionWriter
import indexer.subscriptionapi.subscriptions.subscriptionModule
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.awaitility.kotlin.await
import org.slf4j.LoggerFactory
import java.io.File
import java.time.Duration
import java.util.Properties

/**
 * One real, in-JVM instance of the subscription-api process: the production
 * [IndexerTopology] built into a live [KafkaStreams] instance, its subscriptions
 * producer, and the real Ktor REST module ([module] + [subscriptionModule]) - the
 * exact wiring subscription-api's `main()` performs, only parameterised with test
 * config instead of Hoplite/application.yaml so the harness can start, kill and
 * restart it. Embedded-in-JVM (rather than a subprocess) is deliberate: it is far
 * faster to iterate on and, critically for the 4.6 HA test, lets the harness kill
 * and re-create a Streams instance precisely and observe local Interactive-Query
 * state directly, which a subprocess would hide.
 *
 * Two of these sharing one [applicationId] (with distinct [stateDir] and REST
 * port) form the HA pair the recovery test kills one half of.
 */
class EmbeddedSubscriptionApi(
    val bootstrapServers: String,
    val applicationId: String,
    val httpPort: Int,
    private val networkTopologyConfig: NetworkTopologyConfig,
    private val stateDir: File,
) : AutoCloseable {

    private val log = LoggerFactory.getLogger("harness.EmbeddedSubscriptionApi[$httpPort]")

    private var streams: KafkaStreams? = null
    private var producer: KafkaProducer<String, SubscriptionRecord>? = null
    private var server: io.ktor.server.engine.EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration>? = null

    val baseUrl: String get() = "http://localhost:$httpPort"

    /** Builds a fresh topology + KafkaStreams + REST server and starts them. Idempotent-ish: call [close] first to restart. */
    fun start() {
        stateDir.mkdirs()
        val topology = IndexerTopology.build(networkTopologyConfig, EventDecoder(AbiRegistry()))
        val s = KafkaStreams(topology, streamsProps())
        s.start()
        streams = s

        val p = KafkaProducer(
            producerProps(),
            StringSerializer(),
            jsonSerdeOf(SubscriptionRecord.serializer()).serializer(),
        )
        producer = p

        val deps = AppDependencies(
            subscriptionWriter = KafkaSubscriptionWriter(p, Topics.SUBSCRIPTIONS),
            subscriptionReader = KafkaStreamsSubscriptionReader(s),
            abiRegistry = AbiRegistry(),
        )

        server = embeddedServer(Netty, port = httpPort) {
            module()
            subscriptionModule(deps)
        }.start(wait = false)
        log.info("started (applicationId={}, stateDir={})", applicationId, stateDir)
    }

    /** Blocks until the KafkaStreams instance reaches RUNNING (GlobalKTable restored, IQ answerable). */
    fun awaitRunning(timeout: Duration = Duration.ofSeconds(60)) {
        await.atMost(timeout).until { streams?.state() == KafkaStreams.State.RUNNING }
    }

    fun isRunning(): Boolean = streams?.state() == KafkaStreams.State.RUNNING

    /**
     * Reads this instance's LOCAL confirmation-store entry for [eventKey], or null
     * if absent or not locally hosted (another instance owns that partition) or the
     * store is momentarily un-queryable (mid-rebalance). Lets tests gate
     * deterministically on real lifecycle state instead of wall-clock guesses.
     */
    fun confirmationState(eventKey: String): ConfirmationState? =
        try {
            streams
                ?.store(
                    StoreQueryParameters.fromNameAndType(
                        StoreNames.CONFIRMATION,
                        QueryableStoreTypes.keyValueStore<String, ConfirmationState>(),
                    ),
                )
                ?.get(eventKey)
        } catch (_: Exception) {
            null
        }

    fun blockTrackingState(network: String): BlockTrackingState? =
        try {
            streams
                ?.store(
                    StoreQueryParameters.fromNameAndType(
                        StoreNames.BLOCK_TRACKING,
                        QueryableStoreTypes.keyValueStore<String, BlockTrackingState>(),
                    ),
                )
                ?.get(network)
        } catch (_: Exception) {
            null
        }

    override fun close() {
        try {
            server?.stop(500, 1000)
        } catch (_: Exception) {
        }
        try {
            streams?.close(Duration.ofSeconds(20))
        } catch (_: Exception) {
        }
        try {
            producer?.close()
        } catch (_: Exception) {
        }
        streams = null
        producer = null
        server = null
        log.info("closed")
    }

    private fun streamsProps(): Properties = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1)
        put(StreamsConfig.STATE_DIR_CONFIG, stateDir.absolutePath)
        // Small commit interval so confirmed/invalidated outputs are flushed downstream
        // promptly under test, rather than after the 30s production default.
        put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200)
        // Tighter consumer liveness so a killed instance's partitions are reassigned
        // to the survivor fast in the HA test (still >= the broker's 6s floor).
        put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 10_000)
        put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), 3_000)
    }

    private fun producerProps(): Properties = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ProducerConfig.ACKS_CONFIG, "all")
    }
}
