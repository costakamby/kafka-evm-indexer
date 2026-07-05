package indexer.streamstopology.decode

import indexer.decoder.AbiRegistry
import indexer.decoder.EventDecoder
import indexer.schema.DecodeFailureRecord
import indexer.schema.DecodedEventEnvelope
import indexer.schema.EventKey
import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord
import indexer.schema.SubscriptionRecord
import indexer.schema.SubscriptionStatus
import indexer.streamstopology.Topics
import indexer.streamstopology.serde.jsonSerdeOf
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.TestRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.Properties

/**
 * Component-level wiring test (test pyramid layer 2, TopologyTestDriver): proves
 * the decode step correctly resolves an abiRef via the subscriptions index and
 * branches into decoded-logs-topic / decode-dead-letter-topic (acceptance
 * criterion 4.3). Decode CORRECTNESS itself (nested tuples, BigInteger-as-string)
 * is already exhaustively unit tested in decoder's EventDecoderTest - these
 * tests only prove the topology wiring around it, per the test pyramid's "don't
 * re-prove a lower layer" rule (design doc section 5.2).
 */
class DecodeTopologyTest {

    private lateinit var driver: TopologyTestDriver

    private val stringSerde = Serdes.String()
    private val subscriptionSerde = jsonSerdeOf(SubscriptionRecord.serializer())
    private val rawLogSerde = jsonSerdeOf(RawLogRecord.serializer())
    private val decodedSerde = jsonSerdeOf(DecodedEventEnvelope.serializer())
    private val failureSerde = jsonSerdeOf(DecodeFailureRecord.serializer())

    private val transferTopic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    private fun addressTopic(hex: String): String {
        val clean = hex.removePrefix("0x").lowercase()
        return "0x" + "0".repeat(64 - clean.length) + clean
    }

    private fun buildDriver(): TopologyTestDriver {
        val builder = StreamsBuilder()
        val decoder = EventDecoder(AbiRegistry(), clock = { 1_700_000_000_000L })
        // Mirrors how IndexerTopology registers the subscriptions GlobalKTable
        // before wiring the decode step against its store.
        builder.globalTable(
            Topics.SUBSCRIPTIONS,
            org.apache.kafka.streams.kstream.Consumed.with(stringSerde, subscriptionSerde),
            org.apache.kafka.streams.kstream.Materialized
                .`as`<String, SubscriptionRecord>(
                    org.apache.kafka.streams.state.Stores.persistentKeyValueStore(Topics.SUBSCRIPTIONS_STORE),
                )
                .withKeySerde(stringSerde)
                .withValueSerde(subscriptionSerde),
        )
        DecodeTopology.addTo(
            builder,
            subscriptionsStoreName = Topics.SUBSCRIPTIONS_STORE,
            rawLogsTopic = Topics.RAW_LOGS,
            decodedLogsTopic = Topics.DECODED_LOGS,
            deadLetterTopic = Topics.DECODE_DEAD_LETTER,
            decoder = decoder,
        )
        val props = Properties().apply {
            put(org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG, "decode-topology-test")
            put(org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
        }
        return TopologyTestDriver(builder.build(), props, Instant.ofEpochMilli(0))
    }

    @AfterEach
    fun tearDown() {
        if (::driver.isInitialized) driver.close()
    }

    private fun rawTransferLog(network: String = "ethereum", contract: String = "0xdead") = RawLogRecord(
        network = network,
        contractAddress = contract,
        txHash = "0xtx1",
        logIndex = 0,
        blockNumber = 100,
        blockHash = "0xblk100",
        topics = listOf(transferTopic0, addressTopic("0x01"), addressTopic("0x02")),
        data = "0x0000000000000000000000000000000000000000000000000000000000000064",
        source = IngestionSource.POLL,
        observedAtEpochMillis = 1,
    )

    @Test
    fun `raw log for an ACTIVE subscription is decoded and published to decoded-logs-topic`() {
        driver = buildDriver()
        val subscriptionsIn = driver.createInputTopic(Topics.SUBSCRIPTIONS, stringSerde.serializer(), subscriptionSerde.serializer())
        val rawLogsIn = driver.createInputTopic(Topics.RAW_LOGS, stringSerde.serializer(), rawLogSerde.serializer())
        val decodedOut = driver.createOutputTopic(Topics.DECODED_LOGS, stringSerde.deserializer(), decodedSerde.deserializer())

        subscriptionsIn.pipeInput(
            "sub-1",
            SubscriptionRecord(
                id = "sub-1",
                network = "ethereum",
                address = "0xdead",
                abiRef = "erc20",
                startBlock = null,
                includeEvents = listOf("Transfer"),
                status = SubscriptionStatus.ACTIVE,
                createdAtEpochMillis = 1,
            ),
        )
        rawLogsIn.pipeInput(rawTransferLog())

        val output = decodedOut.readRecordsToList()
        output.size shouldBe 1
        val record = output.single()
        record.key shouldBe EventKey.of("ethereum", "0xtx1", 0)
        record.value.eventName shouldBe "Transfer"
        record.value.network shouldBe "ethereum"
    }

    @Test
    fun `raw log with no matching subscription is dead lettered, never crashes, never silently dropped`() {
        driver = buildDriver()
        val rawLogsIn = driver.createInputTopic(Topics.RAW_LOGS, stringSerde.serializer(), rawLogSerde.serializer())
        val decodedOut = driver.createOutputTopic(Topics.DECODED_LOGS, stringSerde.deserializer(), decodedSerde.deserializer())
        val deadLetterOut = driver.createOutputTopic(Topics.DECODE_DEAD_LETTER, stringSerde.deserializer(), failureSerde.deserializer())

        rawLogsIn.pipeInput(rawTransferLog(contract = "0xunknown"))

        decodedOut.isEmpty shouldBe true
        val failures = deadLetterOut.readRecordsToList()
        failures.size shouldBe 1
        failures.single().value.reason shouldContain "no active subscription"
    }

    @Test
    fun `raw log for a REMOVED subscription is dead lettered, not decoded`() {
        driver = buildDriver()
        val subscriptionsIn = driver.createInputTopic(Topics.SUBSCRIPTIONS, stringSerde.serializer(), subscriptionSerde.serializer())
        val rawLogsIn = driver.createInputTopic(Topics.RAW_LOGS, stringSerde.serializer(), rawLogSerde.serializer())
        val decodedOut = driver.createOutputTopic(Topics.DECODED_LOGS, stringSerde.deserializer(), decodedSerde.deserializer())
        val deadLetterOut = driver.createOutputTopic(Topics.DECODE_DEAD_LETTER, stringSerde.deserializer(), failureSerde.deserializer())

        subscriptionsIn.pipeInput(
            "sub-1",
            SubscriptionRecord(
                id = "sub-1",
                network = "ethereum",
                address = "0xdead",
                abiRef = "erc20",
                status = SubscriptionStatus.REMOVED,
                createdAtEpochMillis = 1,
            ),
        )
        rawLogsIn.pipeInput(rawTransferLog())

        decodedOut.isEmpty shouldBe true
        deadLetterOut.readRecordsToList().single().value.reason shouldContain "REMOVED"
    }

    @Test
    fun `a malformed log for a known ACTIVE subscription is dead lettered via the decoder`() {
        driver = buildDriver()
        val subscriptionsIn = driver.createInputTopic(Topics.SUBSCRIPTIONS, stringSerde.serializer(), subscriptionSerde.serializer())
        val rawLogsIn = driver.createInputTopic(Topics.RAW_LOGS, stringSerde.serializer(), rawLogSerde.serializer())
        val deadLetterOut = driver.createOutputTopic(Topics.DECODE_DEAD_LETTER, stringSerde.deserializer(), failureSerde.deserializer())

        subscriptionsIn.pipeInput(
            "sub-1",
            SubscriptionRecord(
                id = "sub-1",
                network = "ethereum",
                address = "0xdead",
                abiRef = "erc20",
                status = SubscriptionStatus.ACTIVE,
                createdAtEpochMillis = 1,
            ),
        )
        // Transfer needs 2 indexed topics; only supply 1 -> topic count mismatch.
        val badLog = rawTransferLog().copy(topics = listOf(transferTopic0, addressTopic("0x01")))
        rawLogsIn.pipeInput(badLog)

        val failure = deadLetterOut.readRecordsToList().single()
        failure.value.reason shouldContain "topic count mismatch"
    }
}
