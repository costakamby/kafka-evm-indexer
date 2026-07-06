package indexer.integrationtests.harness

import indexer.schema.SubscriptionRecord
import indexer.schema.json.IndexerJson
import indexer.streamstopology.config.NetworkParams
import indexer.streamstopology.config.NetworkTopologyConfig
import indexer.topicadmin.TopicSetup
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Properties

/**
 * The single test-network the integration harness runs against: a locally-forked
 * Anvil chain (chain-id 31337). The real fleet's confirmation depths (design
 * decision 11: ethereum 12, L2s 20, polygon 128) would force a test to mine dozens
 * of blocks to reach CONFIRMED; a dedicated test network with depth 2 keeps the
 * end-to-end confirmation/reorg scenarios fast without weakening what they prove
 * (the SAME production ConfirmationLogic / BlockTrackingLogic runs, just with a
 * smaller threshold). This choice lives ONLY in test config here - it deliberately
 * does NOT touch any module's checked-in application.yaml. Flagged explicitly in
 * the module report.
 */
const val TEST_NETWORK = "testnet"
const val TEST_CONFIRMATION_DEPTH = 2

fun testNetworkTopologyConfig(): NetworkTopologyConfig =
    NetworkTopologyConfig(mapOf(TEST_NETWORK to NetworkParams(confirmationDepth = TEST_CONFIRMATION_DEPTH)))

/** Provisions every topic (idempotently) on [bootstrapServers] before any Streams/producer starts. */
fun ensureTopics(bootstrapServers: String) {
    Admin.create(Properties().apply { put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers) })
        .use { admin -> TopicSetup.ensureTopics(admin) }
}

fun freePort(): Int = ServerSocket(0).use { it.localPort }

/**
 * Tiny REST client for the subscription-api under test, over the JDK HttpClient
 * (no ktor-client-content-negotiation needed). Exercises the real 4.1 contract:
 * POST validates abiRef before producing, GET reads the GlobalKTable, DELETE
 * tombstones.
 */
class SubscriptionApiRestClient(private val baseUrl: String) {
    private val http = HttpClient.newHttpClient()

    data class PostResult(val statusCode: Int, val body: String)

    fun createSubscription(
        network: String,
        address: String,
        abiRef: String,
        startBlock: Long?,
        includeEvents: List<String>,
    ): PostResult {
        val body = buildJsonObject {
            put("network", network)
            put("address", address)
            put("abiRef", abiRef)
            startBlock?.let { put("startBlock", it) }
            putJsonArray("includeEvents") { includeEvents.forEach { add(it) } }
        }
        val request = HttpRequest.newBuilder(URI.create("$baseUrl/subscriptions"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
            .build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        return PostResult(response.statusCode(), response.body())
    }

    fun parseCreated(body: String): SubscriptionRecord =
        IndexerJson.instance.decodeFromString(SubscriptionRecord.serializer(), body)

    fun listActive(network: String): List<SubscriptionRecord> {
        val request = HttpRequest.newBuilder(URI.create("$baseUrl/subscriptions?network=$network&status=ACTIVE")).GET().build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        check(response.statusCode() == 200) { "GET /subscriptions -> ${response.statusCode()}: ${response.body()}" }
        return IndexerJson.instance.decodeFromString(response.body())
    }

    fun delete(id: String): Int {
        val request = HttpRequest.newBuilder(URI.create("$baseUrl/subscriptions/$id")).DELETE().build()
        return http.send(request, HttpResponse.BodyHandlers.ofString()).statusCode()
    }
}
