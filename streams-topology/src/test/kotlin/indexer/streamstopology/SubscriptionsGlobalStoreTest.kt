package indexer.streamstopology

import indexer.decoder.AbiRegistry
import indexer.decoder.EventDecoder
import indexer.schema.SubscriptionRecord
import indexer.schema.SubscriptionStatus
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
 * Proves [IndexerTopology] materializes subscriptions-topic into a GlobalKTable
 * store queryable by subscription id - this is what subscription-api's
 * Interactive-Query-backed GET /subscriptions reads directly (acceptance
 * criterion 4.1: "reads directly from the local GlobalKTable state store... NOT
 * by re-reading the topic per request").
 */
class SubscriptionsGlobalStoreTest {

    private lateinit var driver: TopologyTestDriver

    @AfterEach
    fun tearDown() {
        if (::driver.isInitialized) driver.close()
    }

    @Test
    fun `a produced subscription record is queryable from the subscriptions global store by id`() {
        val topology = IndexerTopology.build(NetworkTopologyConfig(emptyMap()), EventDecoder(AbiRegistry()))
        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "subscriptions-global-store-test")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
        }
        driver = TopologyTestDriver(topology, props, Instant.ofEpochMilli(0))

        val subscriptionsIn = driver.createInputTopic(
            Topics.SUBSCRIPTIONS,
            Serdes.String().serializer(),
            jsonSerdeOf(SubscriptionRecord.serializer()).serializer(),
        )
        val record = SubscriptionRecord(
            id = "sub-1",
            network = "ethereum",
            address = "0xdead",
            abiRef = "erc20",
            status = SubscriptionStatus.ACTIVE,
            createdAtEpochMillis = 1,
        )
        subscriptionsIn.pipeInput("sub-1", record)

        val store = driver.getKeyValueStore<String, SubscriptionRecord>(Topics.SUBSCRIPTIONS_STORE)
        store.get("sub-1") shouldBe record
    }
}
