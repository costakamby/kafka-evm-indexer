package indexer.postgressink.db

import indexer.postgressink.testsupport.EnvelopeFixtures
import indexer.postgressink.testsupport.SharedPostgresContainer
import indexer.schema.ConfirmationStatus
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * Component test (test pyramid layer 3, design doc section 5.2): proves
 * ConfirmedEventRepository's actual JDBC upsert IO against a real Postgres,
 * satisfying the 4.7 acceptance criterion verbatim:
 * "replaying the entire confirmed-events-topic from offset 0 into an empty
 * Postgres produces the same end state as playing it once."
 */
@Testcontainers
class ConfirmedEventRepositoryIdempotencyTest {

    companion object {
        private lateinit var repository: ConfirmedEventRepository

        @JvmStatic
        @BeforeAll
        fun setUp() {
            val dataSource = SharedPostgresContainer.newDataSource()
            repository = ConfirmedEventRepository(dataSource)
            repository.migrate()
        }
    }

    @BeforeEach
    fun emptyTheTable() {
        repository.truncate()
    }

    private val topicContents = listOf(
        EnvelopeFixtures.transfer(network = "ethereum", txHash = "0x1", logIndex = 0, blockNumber = 100),
        EnvelopeFixtures.transfer(network = "ethereum", txHash = "0x1", logIndex = 1, blockNumber = 100),
        EnvelopeFixtures.transfer(network = "ethereum", txHash = "0x2", logIndex = 0, blockNumber = 101),
        EnvelopeFixtures.transfer(network = "polygon", txHash = "0x1", logIndex = 0, blockNumber = 55),
        // a later message correcting an earlier one for the exact same key -
        // still part of "the topic", still must replay idempotently.
        EnvelopeFixtures.transfer(
            network = "ethereum",
            txHash = "0x2",
            logIndex = 0,
            blockNumber = 101,
            status = ConfirmationStatus.INVALIDATED,
        ),
    )

    @Test
    fun `replaying confirmed-events-topic from offset 0 into an empty Postgres produces the same end state as playing it once`() {
        topicContents.forEach { repository.upsert(it) }
        val afterOnePlay = repository.snapshotAll()
        val rowCountAfterOnePlay = repository.countAll()

        // Simulate a full replay from offset 0: the exact same messages,
        // in the exact same order, applied again on top of existing state.
        topicContents.forEach { repository.upsert(it) }
        val afterReplay = repository.snapshotAll()
        val rowCountAfterReplay = repository.countAll()

        rowCountAfterReplay shouldBe rowCountAfterOnePlay
        rowCountAfterReplay shouldBe 4L // 4 distinct (network, tx_hash, log_index) keys, not 5
        afterReplay shouldBe afterOnePlay
    }

    @Test
    fun `upserting the same envelope twice does not create a duplicate row`() {
        val envelope = EnvelopeFixtures.transfer(network = "ethereum", txHash = "0xdupe", logIndex = 3)

        repository.upsert(envelope)
        repository.upsert(envelope)

        repository.countAll() shouldBe 1L
    }
}
