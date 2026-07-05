package indexer.subscriptionapi.subscriptions

import indexer.decoder.AbiRegistry
import indexer.decoder.EventDecoder
import indexer.schema.SubscriptionRecord
import indexer.streamstopology.IndexerTopology
import indexer.streamstopology.Topics
import indexer.streamstopology.config.NetworkTopologyConfig
import indexer.streamstopology.serde.jsonSerdeOf
import indexer.subscriptionapi.module
import io.kotest.matchers.shouldBe
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.Properties

/**
 * Proves acceptance criterion 4.1's GlobalKTable convergence requirement:
 * "GET /subscriptions returns consistent results regardless of which instance
 * answers... write a test proving convergence (produce a record, poll the
 * store until it appears, using Awaitility)". Uses a REAL GlobalKTable, backed
 * by TopologyTestDriver (test pyramid layer 2 - no broker needed to prove the
 * produce -> GlobalKTable-store visibility mechanics), rather than route-level
 * fakes. Proving convergence across multiple SEPARATE running instances needs
 * a real multi-broker/Testcontainers setup - that is integration-tests'
 * responsibility (flagged in the build report), not provable at this layer;
 * what IS provable and is proven here is the single-instance mechanics that
 * make multi-instance convergence true by construction with a GlobalKTable.
 */
class SubscriptionConvergenceTest {

    private lateinit var driver: TopologyTestDriver

    @AfterEach
    fun tearDown() {
        if (::driver.isInitialized) driver.close()
    }

    @Test
    fun `a subscription produced via POST becomes visible via GET once the GlobalKTable store catches up`() {
        val topology = IndexerTopology.build(NetworkTopologyConfig(emptyMap()), EventDecoder(AbiRegistry()))
        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "convergence-test")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
        }
        driver = TopologyTestDriver(topology, props)

        val subscriptionsIn = driver.createInputTopic(
            Topics.SUBSCRIPTIONS,
            Serdes.String().serializer(),
            jsonSerdeOf(SubscriptionRecord.serializer()).serializer(),
        )

        val writer = SubscriptionWriter { record -> subscriptionsIn.pipeInput(record.id, record) }
        val reader = SubscriptionReader {
            val store = driver.getKeyValueStore<String, SubscriptionRecord>(Topics.SUBSCRIPTIONS_STORE)
            store.all().use { it.asSequence().map { kv -> kv.value }.toList() }
        }
        val deps = AppDependencies(writer, reader, AbiRegistry())

        testApplication {
            application { module(); subscriptionModule(deps) }

            val postResponse = client.post("/subscriptions") {
                contentType(ContentType.Application.Json)
                setBody("""{"network":"ethereum","address":"0xdead","abiRef":"erc20","startBlock":null,"includeEvents":[]}""")
            }
            val createdId = Json.parseToJsonElement(postResponse.bodyAsText()).let {
                it.jsonObjectId()
            }

            await.atMost(Duration.ofSeconds(5)).until {
                runBlocking {
                    val getResponse = client.get("/subscriptions")
                    getResponse.bodyAsText().contains(createdId)
                }
            }

            val finalGet = runBlocking { client.get("/subscriptions").bodyAsText() }
            finalGet.contains(createdId) shouldBe true
        }
    }
}

private fun kotlinx.serialization.json.JsonElement.jsonObjectId(): String =
    this.jsonObjectSafe()["id"]!!.jsonPrimitiveSafe().content

private fun kotlinx.serialization.json.JsonElement.jsonObjectSafe() = this as kotlinx.serialization.json.JsonObject
private fun kotlinx.serialization.json.JsonElement.jsonPrimitiveSafe() = this as kotlinx.serialization.json.JsonPrimitive
