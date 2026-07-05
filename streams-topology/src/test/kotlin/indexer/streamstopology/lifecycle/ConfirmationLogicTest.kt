package indexer.streamstopology.lifecycle

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * Pure punctuator-arithmetic unit tests (design doc section 5.2 layer 1 calls
 * this out by name: "punctuator arithmetic"). Traces to acceptance criterion
 * 4.5: confirmation is driven by actual block-tracking state, not wall clock.
 */
class ConfirmationLogicTest {

    @Test
    fun `confirmations seen is the block distance between last tracked block and the event's block`() {
        ConfirmationLogic.confirmationsSeen(lastBlock = 110, eventBlock = 100) shouldBe 10
    }

    @Test
    fun `confirmations seen never goes negative for an event block ahead of last tracked block`() {
        ConfirmationLogic.confirmationsSeen(lastBlock = 95, eventBlock = 100) shouldBe 0
    }

    @Test
    fun `an event exactly at the configured depth is eligible to confirm`() {
        ConfirmationLogic.shouldConfirm(confirmationsSeen = 12, depth = 12) shouldBe true
    }

    @Test
    fun `an event one confirmation short of depth is not yet eligible`() {
        ConfirmationLogic.shouldConfirm(confirmationsSeen = 11, depth = 12) shouldBe false
    }
}
