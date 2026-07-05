package indexer.streamstopology.lifecycle

import indexer.schema.ConfirmationStatus
import indexer.schema.DecodedEventEnvelope
import indexer.schema.EventKey
import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord
import indexer.schema.ktable.ConfirmationState
import indexer.streamstopology.config.NetworkParams
import indexer.streamstopology.config.NetworkTopologyConfig
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Instant

/**
 * TopologyTestDriver tests (test pyramid layer 2) for the core lifecycle:
 * block-tracking reorg detection + confirmation punctuator (4.5), and
 * reconciliation merge/anomaly detection (4.4). No embedded Kafka broker -
 * this is where the bulk of this module's correctness is proven, per design
 * doc section 5.2.
 */
class LifecycleTopologyTest {

    private lateinit var driver: TopologyTestDriver

    private val config = NetworkTopologyConfig(
        mapOf("ethereum" to NetworkParams(confirmationDepth = 12, reconciliationGapBlocks = 3)),
    )

    @AfterEach
    fun tearDown() {
        if (::driver.isInitialized) driver.close()
    }

    private fun rawLog(network: String, blockNumber: Long, blockHash: String, ts: Instant) = RawLogRecord(
        network = network,
        contractAddress = "0xc",
        txHash = "0xtx-block-marker-$blockNumber",
        logIndex = 0,
        blockNumber = blockNumber,
        blockHash = blockHash,
        topics = listOf("0xtopic0"),
        data = "0x",
        source = IngestionSource.POLL,
        observedAtEpochMillis = ts.toEpochMilli(),
    )

    private fun decodedEvent(
        network: String = "ethereum",
        txHash: String = "0xtx1",
        blockNumber: Long = 100,
        source: IngestionSource = IngestionSource.POLL,
        value: String = "42",
    ) = DecodedEventEnvelope(
        eventName = "Transfer",
        signatureHash = "0xtopic0",
        network = network,
        contractAddress = "0xc",
        txHash = txHash,
        logIndex = 0,
        blockNumber = blockNumber,
        status = ConfirmationStatus.UNCONFIRMED,
        source = source,
        decodedFields = JsonObject(mapOf("value" to JsonPrimitive(value))),
    )

    @Test
    fun `an event seen by both ws and poll produces exactly one merged confirmation-store entry`() {
        driver = LifecycleTestTopology.newDriver(config)
        val decodedIn = driver.createInputTopic(
            LifecycleTestTopology.DECODED_LOGS,
            LifecycleTestTopology.stringSerde.serializer(),
            LifecycleTestTopology.decodedSerde.serializer(),
        )
        val key = EventKey.of("ethereum", "0xtx1", 0)

        decodedIn.pipeInput(key, decodedEvent(source = IngestionSource.WS), Instant.ofEpochMilli(1000))
        decodedIn.pipeInput(key, decodedEvent(source = IngestionSource.POLL), Instant.ofEpochMilli(2000))

        val confirmationStore = driver.getKeyValueStore<String, ConfirmationState>(StoreNames.CONFIRMATION)
        val reconciliationStore = driver.getKeyValueStore<String, indexer.schema.ktable.ReconciliationState>(StoreNames.RECONCILIATION)

        // Exactly one confirmation-store entry for this key (not two competing ones).
        confirmationStore.get(key).let { state ->
            state shouldBe state // sanity: non-null below
        }
        confirmationStore.get(key)!!.status shouldBe ConfirmationStatus.UNCONFIRMED
        // Reconciliation's job is done once both sides corroborate - tombstoned, not left to grow forever.
        reconciliationStore.get(key) shouldBe null
    }

    @Test
    fun `poll-seen-never-ws is flagged ws_gap_suspected on reconciliation-anomalies-topic once the gap window elapses`() {
        driver = LifecycleTestTopology.newDriver(config)
        val decodedIn = driver.createInputTopic(
            LifecycleTestTopology.DECODED_LOGS,
            LifecycleTestTopology.stringSerde.serializer(),
            LifecycleTestTopology.decodedSerde.serializer(),
        )
        val rawIn = driver.createInputTopic(
            LifecycleTestTopology.RAW_LOGS,
            LifecycleTestTopology.stringSerde.serializer(),
            LifecycleTestTopology.rawLogSerde.serializer(),
        )
        val anomaliesOut = driver.createOutputTopic(
            LifecycleTestTopology.ANOMALIES,
            LifecycleTestTopology.stringSerde.deserializer(),
            LifecycleTestTopology.anomalySerde.deserializer(),
        )

        val key = EventKey.of("ethereum", "0xtx1", 0)
        decodedIn.pipeInput(key, decodedEvent(source = IngestionSource.POLL, blockNumber = 100), Instant.ofEpochMilli(1000))

        // Advance block-tracking (and stream time, to fire the punctuator) to block 100, 101, 102, 103.
        for (b in 100L..103L) {
            rawIn.pipeInput("ethereum", rawLog("ethereum", b, "0xhash$b", Instant.ofEpochMilli(1000 + b * 1000)), Instant.ofEpochMilli(1000 + b * 1000))
        }

        val anomalies = anomaliesOut.readRecordsToList()
        anomalies.size shouldBe 1
        anomalies.single().value.type shouldBe indexer.schema.ReconciliationAnomalyType.WS_GAP_SUSPECTED

        // Single-shot: the reconciliation-store entry is gone, so it can't re-fire on later advances.
        val reconciliationStore = driver.getKeyValueStore<String, indexer.schema.ktable.ReconciliationState>(StoreNames.RECONCILIATION)
        reconciliationStore.get(key) shouldBe null
    }

    @Test
    fun `ws-seen-never-corroborated-by-poll is flagged poll_only_confirmed`() {
        driver = LifecycleTestTopology.newDriver(config)
        val decodedIn = driver.createInputTopic(
            LifecycleTestTopology.DECODED_LOGS,
            LifecycleTestTopology.stringSerde.serializer(),
            LifecycleTestTopology.decodedSerde.serializer(),
        )
        val rawIn = driver.createInputTopic(
            LifecycleTestTopology.RAW_LOGS,
            LifecycleTestTopology.stringSerde.serializer(),
            LifecycleTestTopology.rawLogSerde.serializer(),
        )
        val anomaliesOut = driver.createOutputTopic(
            LifecycleTestTopology.ANOMALIES,
            LifecycleTestTopology.stringSerde.deserializer(),
            LifecycleTestTopology.anomalySerde.deserializer(),
        )

        decodedIn.pipeInput(
            EventKey.of("ethereum", "0xtx1", 0),
            decodedEvent(source = IngestionSource.WS, blockNumber = 100),
            Instant.ofEpochMilli(1000),
        )
        for (b in 100L..103L) {
            rawIn.pipeInput("ethereum", rawLog("ethereum", b, "0xhash$b", Instant.ofEpochMilli(1000 + b * 1000)), Instant.ofEpochMilli(1000 + b * 1000))
        }

        val anomalies = anomaliesOut.readRecordsToList()
        anomalies.size shouldBe 1
        anomalies.single().value.type shouldBe indexer.schema.ReconciliationAnomalyType.POLL_ONLY_CONFIRMED
    }

    @Test
    fun `UNCONFIRMED promotes to CONFIRMED only once block-tracking reaches the configured depth, not before`() {
        driver = LifecycleTestTopology.newDriver(config)
        val decodedIn = driver.createInputTopic(
            LifecycleTestTopology.DECODED_LOGS,
            LifecycleTestTopology.stringSerde.serializer(),
            LifecycleTestTopology.decodedSerde.serializer(),
        )
        val rawIn = driver.createInputTopic(
            LifecycleTestTopology.RAW_LOGS,
            LifecycleTestTopology.stringSerde.serializer(),
            LifecycleTestTopology.rawLogSerde.serializer(),
        )
        val confirmedOut = driver.createOutputTopic(
            LifecycleTestTopology.CONFIRMED_EVENTS,
            LifecycleTestTopology.stringSerde.deserializer(),
            LifecycleTestTopology.decodedSerde.deserializer(),
        )

        val key = EventKey.of("ethereum", "0xtx1", 0)
        decodedIn.pipeInput(key, decodedEvent(blockNumber = 100), Instant.ofEpochMilli(1000))

        // Advance to block 111 - 11 confirmations, one short of depth=12. Must NOT confirm yet.
        for (b in 100L..111L) {
            rawIn.pipeInput("ethereum", rawLog("ethereum", b, "0xhash$b", Instant.ofEpochMilli(1000 + b * 1000)), Instant.ofEpochMilli(1000 + b * 1000))
        }
        confirmedOut.isEmpty shouldBe true

        // One more block -> exactly depth=12 confirmations -> now it must confirm.
        rawIn.pipeInput("ethereum", rawLog("ethereum", 112L, "0xhash112", Instant.ofEpochMilli(1000 + 112_000)), Instant.ofEpochMilli(1000 + 112_000))

        val confirmed = confirmedOut.readRecordsToList()
        confirmed.size shouldBe 1
        confirmed.single().key shouldBe key
        confirmed.single().value.status shouldBe ConfirmationStatus.CONFIRMED
    }

    @Test
    fun `a reorg at an unconfirmed event's height marks it INVALIDATED before it ever reaches CONFIRMED`() {
        driver = LifecycleTestTopology.newDriver(config)
        val decodedIn = driver.createInputTopic(
            LifecycleTestTopology.DECODED_LOGS,
            LifecycleTestTopology.stringSerde.serializer(),
            LifecycleTestTopology.decodedSerde.serializer(),
        )
        val rawIn = driver.createInputTopic(
            LifecycleTestTopology.RAW_LOGS,
            LifecycleTestTopology.stringSerde.serializer(),
            LifecycleTestTopology.rawLogSerde.serializer(),
        )
        val confirmedOut = driver.createOutputTopic(
            LifecycleTestTopology.CONFIRMED_EVENTS,
            LifecycleTestTopology.stringSerde.deserializer(),
            LifecycleTestTopology.decodedSerde.deserializer(),
        )

        val key = EventKey.of("ethereum", "0xtx1", 0)
        // Event first observed at block 101.
        rawIn.pipeInput("ethereum", rawLog("ethereum", 100L, "0xhash100", Instant.ofEpochMilli(1000)), Instant.ofEpochMilli(1000))
        rawIn.pipeInput("ethereum", rawLog("ethereum", 101L, "0xhash101", Instant.ofEpochMilli(2000)), Instant.ofEpochMilli(2000))
        decodedIn.pipeInput(key, decodedEvent(blockNumber = 101), Instant.ofEpochMilli(2000))

        // Only 2 confirmations so far - still well within the UNCONFIRMED window (depth=12).
        rawIn.pipeInput("ethereum", rawLog("ethereum", 102L, "0xhash102", Instant.ofEpochMilli(3000)), Instant.ofEpochMilli(3000))

        // Reorg: block 101's canonical hash changes before this event ever reaches CONFIRMED.
        rawIn.pipeInput("ethereum", rawLog("ethereum", 101L, "0xhash101-fork", Instant.ofEpochMilli(4000)), Instant.ofEpochMilli(4000))

        val confirmed = confirmedOut.readRecordsToList()
        confirmed.size shouldBe 1
        confirmed.single().key shouldBe key
        confirmed.single().value.status shouldBe ConfirmationStatus.INVALIDATED
    }

    @Test
    fun `confirmation-store does not grow unbounded - terminal entries are swept well past the confirmation depth`() {
        driver = LifecycleTestTopology.newDriver(config)
        val decodedIn = driver.createInputTopic(
            LifecycleTestTopology.DECODED_LOGS,
            LifecycleTestTopology.stringSerde.serializer(),
            LifecycleTestTopology.decodedSerde.serializer(),
        )
        val rawIn = driver.createInputTopic(
            LifecycleTestTopology.RAW_LOGS,
            LifecycleTestTopology.stringSerde.serializer(),
            LifecycleTestTopology.rawLogSerde.serializer(),
        )

        val key = EventKey.of("ethereum", "0xtx1", 0)
        decodedIn.pipeInput(key, decodedEvent(blockNumber = 100), Instant.ofEpochMilli(1000))

        // Advance well past confirmation (depth=12) and past the terminal sweep buffer.
        for (b in 100L..140L) {
            rawIn.pipeInput("ethereum", rawLog("ethereum", b, "0xhash$b", Instant.ofEpochMilli(1000 + b * 1000)), Instant.ofEpochMilli(1000 + b * 1000))
        }

        val confirmationStore = driver.getKeyValueStore<String, ConfirmationState>(StoreNames.CONFIRMATION)
        confirmationStore.get(key) shouldBe null
    }
}
