package indexer.ingestionpoll.rpc

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * Pure exponential-backoff arithmetic backing the rate-limit retry behaviour
 * required by design doc 4.2 ("handles provider rate-limit errors with
 * backoff"). Layer 1 of the test pyramid - no IO at all.
 */
class BackoffTest {

    @Test
    fun `doubles the delay on each successive attempt`() {
        Backoff.delayMillis(attempt = 0, initialDelayMs = 100, maxDelayMs = 10_000) shouldBe 100
        Backoff.delayMillis(attempt = 1, initialDelayMs = 100, maxDelayMs = 10_000) shouldBe 200
        Backoff.delayMillis(attempt = 2, initialDelayMs = 100, maxDelayMs = 10_000) shouldBe 400
        Backoff.delayMillis(attempt = 3, initialDelayMs = 100, maxDelayMs = 10_000) shouldBe 800
    }

    @Test
    fun `caps the delay at maxDelayMs and never overflows`() {
        Backoff.delayMillis(attempt = 10, initialDelayMs = 100, maxDelayMs = 5_000) shouldBe 5_000
        Backoff.delayMillis(attempt = 62, initialDelayMs = 100, maxDelayMs = 5_000) shouldBe 5_000
    }

    @Test
    fun `rejects a negative attempt`() {
        try {
            Backoff.delayMillis(attempt = -1, initialDelayMs = 100, maxDelayMs = 5_000)
            error("expected IllegalArgumentException")
        } catch (_: IllegalArgumentException) {
            // expected
        }
    }
}
