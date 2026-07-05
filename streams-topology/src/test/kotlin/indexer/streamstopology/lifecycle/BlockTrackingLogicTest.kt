package indexer.streamstopology.lifecycle

import indexer.schema.ktable.BlockHashEntry
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * Pure unit tests (test pyramid layer 1: no Kafka) for block-hash ancestry
 * maintenance and reorg detection - the arithmetic core of acceptance
 * criterion 4.5's "Block-tracking KTable correctly maintains recent block hash
 * ancestry ... sufficient to detect reorgs up to the configured depth."
 */
class BlockTrackingLogicTest {

    @Test
    fun `first block observed for a network initializes tracking state`() {
        val result = BlockTrackingLogic.applyNewBlock(null, "ethereum", 100, "0xhashA", depth = 12)

        result.reorgAtHeight shouldBe null
        result.state.network shouldBe "ethereum"
        result.state.lastBlock shouldBe 100
        result.state.recentBlockHashes shouldBe listOf(BlockHashEntry(100, "0xhashA"))
        result.state.reorgDepthWatermark shouldBe 12
    }

    @Test
    fun `a new higher block extends ancestry and advances lastBlock`() {
        val first = BlockTrackingLogic.applyNewBlock(null, "ethereum", 100, "0xhashA", depth = 12).state
        val second = BlockTrackingLogic.applyNewBlock(first, "ethereum", 101, "0xhashB", depth = 12)

        second.reorgAtHeight shouldBe null
        second.state.lastBlock shouldBe 101
        second.state.recentBlockHashes shouldBe listOf(
            BlockHashEntry(100, "0xhashA"),
            BlockHashEntry(101, "0xhashB"),
        )
    }

    @Test
    fun `re-observing the same height with the same hash is a no-op`() {
        val first = BlockTrackingLogic.applyNewBlock(null, "ethereum", 100, "0xhashA", depth = 12).state
        val second = BlockTrackingLogic.applyNewBlock(first, "ethereum", 100, "0xhashA", depth = 12)

        second.reorgAtHeight shouldBe null
        second.state shouldBe first
    }

    @Test
    fun `a different hash at an already-seen height is detected as a reorg and rolls tracking back to that height`() {
        var state = BlockTrackingLogic.applyNewBlock(null, "ethereum", 100, "0xhashA", depth = 12).state
        state = BlockTrackingLogic.applyNewBlock(state, "ethereum", 101, "0xhashB", depth = 12).state
        state = BlockTrackingLogic.applyNewBlock(state, "ethereum", 102, "0xhashC", depth = 12).state

        // Chain reorganizes at height 101: a competing block replaces 0xhashB.
        val reorgResult = BlockTrackingLogic.applyNewBlock(state, "ethereum", 101, "0xhashB-fork", depth = 12)

        reorgResult.reorgAtHeight shouldBe 101L
        // Anything cached above the reorged height (102/0xhashC, from the stale fork) is dropped.
        reorgResult.state.recentBlockHashes shouldBe listOf(
            BlockHashEntry(100, "0xhashA"),
            BlockHashEntry(101, "0xhashB-fork"),
        )
        reorgResult.state.lastBlock shouldBe 101
    }

    @Test
    fun `ancestry window is bounded and does not grow unboundedly beyond depth`() {
        var state: indexer.schema.ktable.BlockTrackingState? = null
        for (i in 1..500L) {
            state = BlockTrackingLogic.applyNewBlock(state, "ethereum", i, "0xhash$i", depth = 12).state
        }

        // Bounded window: must not retain all 500 historical entries.
        (state!!.recentBlockHashes.size < 100) shouldBe true
        state.lastBlock shouldBe 500
        // Must still retain enough ancestry to cover the configured depth.
        (state.recentBlockHashes.size >= 12) shouldBe true
    }
}
