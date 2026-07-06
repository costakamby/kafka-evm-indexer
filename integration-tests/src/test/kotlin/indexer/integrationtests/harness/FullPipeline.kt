package indexer.integrationtests.harness

import indexer.integrationtests.fixtures.AnvilFixture
import java.io.File
import java.nio.file.Files
import java.time.Duration
import java.util.UUID
import javax.sql.DataSource

/**
 * Composes the whole pipeline in one JVM against a given Testcontainers Kafka,
 * shared Postgres and shared Anvil: one subscription-api instance (Ktor +
 * KafkaStreams: decode + reconciliation + block-tracking + confirmation), a
 * postgres-sink, an ingestion-poll and (optionally) an ingestion-ws, all wired to
 * the same broker. This is the single-instance harness used by the reorg (4.5)
 * and happy-path smoke tests; the HA recovery test (4.6) composes its own two
 * subscription-api instances directly.
 */
class FullPipeline(
    bootstrapServers: String,
    anvil: AnvilFixture,
    dataSource: DataSource,
    network: String = TEST_NETWORK,
    includeWs: Boolean = true,
) : AutoCloseable {

    private val runId = UUID.randomUUID().toString().take(8)
    private val stateDir: File = Files.createTempDirectory("itest-state-$runId").toFile()

    val api = EmbeddedSubscriptionApi(
        bootstrapServers = bootstrapServers,
        applicationId = "itest-subapi-$runId",
        httpPort = freePort(),
        networkTopologyConfig = testNetworkTopologyConfig(),
        stateDir = stateDir,
    )
    val sink = EmbeddedPostgresSink(bootstrapServers, dataSource, groupId = "itest-sink-$runId")
    val poller = EmbeddedPoller(bootstrapServers, anvil.httpRpcUrl, api.baseUrl, network)
    private val ws: EmbeddedWsIngestion? =
        if (includeWs) {
            EmbeddedWsIngestion(bootstrapServers, anvil.wsRpcUrl, anvil.httpRpcUrl, api.baseUrl, network)
        } else {
            null
        }

    val rest = SubscriptionApiRestClient(api.baseUrl)
    val repository get() = sink.repository

    fun start() {
        ensureTopics(api.bootstrapServers)
        api.start()
        api.awaitRunning(Duration.ofSeconds(60))
        sink.start()
        poller.start()
        ws?.start()
    }

    override fun close() {
        try {
            poller.close()
        } catch (_: Exception) {
        }
        try {
            ws?.close()
        } catch (_: Exception) {
        }
        try {
            sink.close()
        } catch (_: Exception) {
        }
        try {
            api.close()
        } catch (_: Exception) {
        }
        stateDir.deleteRecursively()
    }
}
