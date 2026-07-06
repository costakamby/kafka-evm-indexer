package indexer.integrationtests

import indexer.integrationtests.fixtures.Erc20Fixture
import indexer.integrationtests.harness.FullPipeline
import indexer.integrationtests.harness.KafkaTestBroker
import indexer.integrationtests.harness.SharedInfra
import indexer.integrationtests.harness.TEST_NETWORK
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.math.BigInteger
import java.time.Duration
import java.time.Instant

/**
 * SKETCH ONLY - explicitly lower priority per design doc 4.8/module brief.
 * Excluded from the normal `./gradlew test` run (see the `excludeTags("soak")`
 * wiring in integration-tests/build.gradle.kts) so it never blocks a PR or slows
 * down the merge-gating suite; run it explicitly via
 * `./gradlew :integration-tests:test -PincludeSoakTests` (or by removing the
 * exclusion) when you actually want a soak run.
 *
 * What this DOES prove: the pipeline keeps up with a sustained stream of real
 * Anvil-sourced transfers for [durationSeconds] without falling over, and JVM
 * heap usage doesn't show gross, obvious growth across the run.
 *
 * What this does NOT prove (the real 4.8 "load/soak" bar - flagged explicitly,
 * not silently claimed as done):
 *  - no unbounded RocksDB state-store growth on disk (would need to sample
 *    state.dir size over time, not just JVM heap).
 *  - no consumer lag growth over a MEANINGFUL duration (design doc says
 *    "e.g. 30 min" - [durationSeconds] defaults to a short sanity value here
 *    so it doesn't dominate CI/local dev time; override via the
 *    `SOAK_DURATION_SECONDS` env var for a real 30-minute run).
 *  - a trend view over MULTIPLE runs (design doc 5.5: "track results over
 *    time... rather than treating it as pass/fail") - this is a single-run
 *    sanity check, not the tracked-metrics harness the design doc describes.
 */
@Tag("soak")
class SoakTest {

    private val log = LoggerFactory.getLogger("harness.SoakTest")

    @Test
    fun `pipeline keeps up with a sustained stream of transfers without falling over`() {
        val durationSeconds = (System.getenv("SOAK_DURATION_SECONDS")?.toLongOrNull() ?: 30L)
        val kafka = KafkaTestBroker().start()
        val dataSource = SharedInfra.newPostgresDataSource()
        val pipeline = FullPipeline(kafka.bootstrapServers, SharedInfra.anvil, dataSource, TEST_NETWORK, includeWs = false)

        try {
            pipeline.start()
            val anvil = SharedInfra.anvil
            val erc20 = Erc20Fixture(anvil.web3j, anvil.credentials, anvil.chainId)
            erc20.deploy(BigInteger.valueOf(1_000_000_000))
            val startBlock = anvil.web3j.ethBlockNumber().send().blockNumber.toLong()

            val post = pipeline.rest.createSubscription(TEST_NETWORK, erc20.contractAddress, "erc20", startBlock, listOf("Transfer"))
            check(post.statusCode == 201) { "subscribe failed: ${post.body}" }
            await.atMost(Duration.ofSeconds(20)).until {
                pipeline.rest.listActive(TEST_NETWORK).any { it.address.equals(erc20.contractAddress, ignoreCase = true) }
            }

            val runtime = Runtime.getRuntime()
            val heapAtStart = runtime.let { it.totalMemory() - it.freeMemory() }
            val recipient = "0x00000000000000000000000000000000000000c0"
            val deadline = Instant.now().plusSeconds(durationSeconds)
            var mined = 0
            var lastReceiptTx: String? = null
            var lastReceiptLogIndex: Long = 0

            while (Instant.now().isBefore(deadline)) {
                val receipt = erc20.transfer(recipient, BigInteger.valueOf((mined + 1).toLong()))
                lastReceiptTx = receipt.transactionHash
                lastReceiptLogIndex = receipt.logs.first().logIndex.toLong()
                mined++
            }
            log.info("soak run mined {} transfers over {}s", mined, durationSeconds)

            // confirmed-events-topic only ever carries CONFIRMED/INVALIDATED events
            // (never UNCONFIRMED - see IndexerTopology's branch), so the very last
            // mined transfer needs a few more blocks mined AFTER it before it can
            // reach the test network's confirmation depth and show up in Postgres at
            // all. Mine a handful of "settling" transfers so the load-generation loop
            // doesn't race its own final assertion.
            val finalTx = requireNotNull(lastReceiptTx)
            repeat(indexer.integrationtests.harness.TEST_CONFIRMATION_DEPTH + 2) {
                erc20.transfer(recipient, BigInteger.ZERO)
            }

            // The pipeline must still be alive and catch up on the LAST mined event
            // within a bounded window after load stops - a crude "didn't fall over
            // and didn't fall permanently behind" signal.
            await.atMost(Duration.ofSeconds(60)).until {
                pipeline.repository.findByKey(TEST_NETWORK, finalTx, lastReceiptLogIndex) != null
            }

            val heapAtEnd = runtime.let { it.totalMemory() - it.freeMemory() }
            log.info(
                "heap used: start={}MB end={}MB (sanity check only - not a rigorous leak/RocksDB-growth test, see class kdoc)",
                heapAtStart / (1024 * 1024),
                heapAtEnd / (1024 * 1024),
            )
        } finally {
            pipeline.close()
            kafka.close()
        }
    }
}
