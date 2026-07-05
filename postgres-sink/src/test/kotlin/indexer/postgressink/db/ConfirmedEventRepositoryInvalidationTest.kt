package indexer.postgressink.db

import indexer.postgressink.testsupport.EnvelopeFixtures
import indexer.postgressink.testsupport.SharedPostgresContainer
import indexer.schema.ConfirmationStatus
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * Component test (test pyramid layer 3, design doc section 5.2): proves the
 * other explicit 4.7 acceptance criterion - "an INVALIDATED correction
 * message updates the corresponding Postgres row rather than leaving a
 * stale CONFIRMED row behind" - against a real Postgres.
 */
@Testcontainers
class ConfirmedEventRepositoryInvalidationTest {

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

    @Test
    fun `an INVALIDATED correction updates the existing row in place instead of leaving a stale CONFIRMED row`() {
        val network = "ethereum"
        val txHash = "0xreorged"
        val logIndex = 2L

        val confirmed = EnvelopeFixtures.transfer(
            network = network,
            txHash = txHash,
            logIndex = logIndex,
            status = ConfirmationStatus.CONFIRMED,
        )
        repository.upsert(confirmed)

        val afterConfirm = repository.findByKey(network, txHash, logIndex)
        afterConfirm shouldNotBe null
        afterConfirm!!.status shouldBe ConfirmationStatus.CONFIRMED

        val invalidated = confirmed.copy(status = ConfirmationStatus.INVALIDATED)
        repository.upsert(invalidated)

        // exactly one row for this key - not a stale CONFIRMED row alongside a new one
        repository.countForKey(network, txHash, logIndex) shouldBe 1L

        val afterInvalidate = repository.findByKey(network, txHash, logIndex)
        afterInvalidate shouldNotBe null
        afterInvalidate!!.status shouldBe ConfirmationStatus.INVALIDATED
    }
}
