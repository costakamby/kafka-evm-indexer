package indexer.ingestionpoll.poll

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * Pure block-range chunking + backfill-vs-incremental decision logic backing
 * design doc 4.2's "adding a new subscription with a start_block in the past
 * triggers a correct historical backfill" and "respects configurable batch
 * size / block range per call" bullets. Layer 1 of the test pyramid.
 */
class BlockRangePlannerTest {

    @Test
    fun `chunks a range into maxBlockRange-sized pieces`() {
        val ranges = BlockRangePlanner.plan(fromBlock = 100, toBlock = 249, maxBlockRange = 50)

        ranges shouldBe listOf(
            BlockRange(100, 149),
            BlockRange(150, 199),
            BlockRange(200, 249),
        )
    }

    @Test
    fun `a single short range fits in one chunk`() {
        val ranges = BlockRangePlanner.plan(fromBlock = 10, toBlock = 15, maxBlockRange = 2000)

        ranges shouldBe listOf(BlockRange(10, 15))
    }

    @Test
    fun `an empty range (fromBlock greater than toBlock) yields no chunks`() {
        val ranges = BlockRangePlanner.plan(fromBlock = 500, toBlock = 499, maxBlockRange = 100)

        ranges shouldBe emptyList()
    }

    @Test
    fun `rejects a non-positive maxBlockRange`() {
        try {
            BlockRangePlanner.plan(fromBlock = 0, toBlock = 10, maxBlockRange = 0)
            error("expected IllegalArgumentException")
        } catch (_: IllegalArgumentException) {
            // expected
        }
    }

    @Test
    fun `resolveStartBlock resumes from watermark plus one when progress is already tracked`() {
        val start = BlockRangePlanner.resolveStartBlock(
            lastPolledBlock = 900,
            subscriptionStartBlock = 100,
            currentHead = 1000,
        )

        start shouldBe 901
    }

    @Test
    fun `resolveStartBlock backfills from the subscription's start_block for a brand-new subscription`() {
        val start = BlockRangePlanner.resolveStartBlock(
            lastPolledBlock = null,
            subscriptionStartBlock = 700,
            currentHead = 1000,
        )

        start shouldBe 700
    }

    @Test
    fun `resolveStartBlock defaults to the current head when no watermark and no start_block are known`() {
        val start = BlockRangePlanner.resolveStartBlock(
            lastPolledBlock = null,
            subscriptionStartBlock = null,
            currentHead = 1000,
        )

        start shouldBe 1000
    }
}
