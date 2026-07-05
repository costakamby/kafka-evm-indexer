package indexer.ingestionws.ws

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * Unit test (pyramid layer 1): pure computation of the exact eth_getLogs
 * catch-up range for a WS reconnect (design decision 7 / acceptance
 * criterion 4.2 "issues an eth_getLogs catch-up call for the exact gap
 * range"). No off-by-one: the range must be [lastSeenBlock+1, currentHead].
 */
class CatchupRangeTest {

    @Test
    fun `no prior state means nothing to catch up`() {
        CatchupRange.compute(lastSeenBlock = null, currentHead = 100L) shouldBe null
    }

    @Test
    fun `already at head means nothing to catch up`() {
        CatchupRange.compute(lastSeenBlock = 100L, currentHead = 100L) shouldBe null
    }

    @Test
    fun `computes the exact inclusive gap range`() {
        CatchupRange.compute(lastSeenBlock = 100L, currentHead = 105L) shouldBe 101L..105L
    }

    @Test
    fun `a single missed block yields a single-block range`() {
        CatchupRange.compute(lastSeenBlock = 100L, currentHead = 101L) shouldBe 101L..101L
    }
}
