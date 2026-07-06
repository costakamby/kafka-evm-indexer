package indexer.streamstopology

import indexer.decoder.AbiRegistry
import indexer.decoder.EventDecoder
import indexer.schema.ConfirmationStatus
import indexer.schema.DecodedEventEnvelope
import indexer.schema.EventKey
import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord
import indexer.schema.SubscriptionRecord
import indexer.schema.SubscriptionStatus
import indexer.streamstopology.config.NetworkParams
import indexer.streamstopology.config.NetworkTopologyConfig
import indexer.streamstopology.serde.jsonSerdeOf
import io.kotest.matchers.shouldBe
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.Properties

/**
 * End-to-end TopologyTestDriver test (still test pyramid layer 2 - no broker)
 * proving genuine cross-piece wiring: a raw log flows through decode,
 * reconciliation, and confirmation all the way to confirmed-events-topic. Each
 * piece already has focused unit/wiring tests (DecodeTopologyTest,
 * LifecycleTopologyTest, EventDecoderTest); this test's only job is to prove
 * they compose correctly end-to-end when assembled by [IndexerTopology] - the
 * exact shape subscription-api will run in production.
 */
class IndexerTopologyEndToEndTest {

    private lateinit var driver: TopologyTestDriver

    private val stringSerde = Serdes.String()
    private val subscriptionSerde = jsonSerdeOf(SubscriptionRecord.serializer())
    private val rawLogSerde = jsonSerdeOf(RawLogRecord.serializer())
    private val decodedSerde = jsonSerdeOf(DecodedEventEnvelope.serializer())

    private val transferTopic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    private fun addressTopic(hex: String): String {
        val clean = hex.removePrefix("0x").lowercase()
        return "0x" + "0".repeat(64 - clean.length) + clean
    }

    @AfterEach
    fun tearDown() {
        if (::driver.isInitialized) driver.close()
    }

    @Test
    fun `a raw log for an active subscription flows through decode, reconciliation and confirmation to confirmed-events-topic`() {
        val config = NetworkTopologyConfig(mapOf("ethereum" to NetworkParams(confirmationDepth = 5)))
        val decoder = EventDecoder(AbiRegistry())
        val topology = IndexerTopology.build(config, decoder)

        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "indexer-topology-e2e-test")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
        }
        driver = TopologyTestDriver(topology, props, Instant.ofEpochMilli(0))

        val subscriptionsIn = driver.createInputTopic(Topics.SUBSCRIPTIONS, stringSerde.serializer(), subscriptionSerde.serializer())
        val rawLogsIn = driver.createInputTopic(Topics.RAW_LOGS, stringSerde.serializer(), rawLogSerde.serializer())
        val confirmedOut = driver.createOutputTopic(Topics.CONFIRMED_EVENTS, stringSerde.deserializer(), decodedSerde.deserializer())

        subscriptionsIn.pipeInput(
            "sub-1",
            SubscriptionRecord(
                id = "sub-1",
                network = "ethereum",
                address = "0xdead",
                abiRef = "erc20",
                includeEvents = listOf("Transfer"),
                status = SubscriptionStatus.ACTIVE,
                createdAtEpochMillis = 1,
            ),
            Instant.ofEpochMilli(500),
        )

        val eventLog = RawLogRecord(
            network = "ethereum",
            contractAddress = "0xdead",
            txHash = "0xtx1",
            logIndex = 0,
            blockNumber = 100,
            blockHash = "0xblk100",
            topics = listOf(transferTopic0, addressTopic("0x01"), addressTopic("0x02")),
            data = "0x0000000000000000000000000000000000000000000000000000000000000064",
            source = IngestionSource.POLL,
            observedAtEpochMillis = 1000,
        )
        rawLogsIn.pipeInput(eventLog, Instant.ofEpochMilli(1000))

        // Advance block-tracking on ethereum past the configured depth (5): blocks
        // 101..106. Depth 5 is reached once lastBlock=105 (105-100=5), but
        // TopologyTestDriver's STREAM_TIME punctuator evaluates trailing/at rest -
        // its schedule fires on the NEXT stream-time advance after a threshold is
        // crossed, so the promotion triggered by lastBlock=105 is only actually
        // evaluated once a further record (106) advances stream time again. This
        // never matters in production (blocks keep arriving continuously); it's
        // purely a synthetic-test artifact of stopping input at exactly the
        // threshold, so one extra marker block is piped to observe the promotion
        // this test asserts on.
        for (b in 101L..106L) {
            val marker = eventLog.copy(txHash = "0xtx-marker-$b", logIndex = 1, blockNumber = b, blockHash = "0xblk$b")
            rawLogsIn.pipeInput(marker, Instant.ofEpochMilli(1000 + b * 1000))
        }

        val confirmed = confirmedOut.readRecordsToList()
        confirmed.size shouldBe 1
        val record = confirmed.single()
        record.key shouldBe EventKey.of("ethereum", "0xtx1", 0)
        record.value.status shouldBe ConfirmationStatus.CONFIRMED
        record.value.eventName shouldBe "Transfer"
        record.value.decodedFields["value"].toString() shouldBe "\"100\""
    }
}
