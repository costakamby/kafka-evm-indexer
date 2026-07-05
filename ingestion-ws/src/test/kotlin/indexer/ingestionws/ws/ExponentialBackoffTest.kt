package indexer.ingestionws.ws

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Unit test (pyramid layer 1): pure backoff arithmetic used between WS
 * reconnect attempts (design decision 7 / acceptance criterion 4.2).
 */
class ExponentialBackoffTest {

    @Test
    fun `first attempt waits the initial delay`() {
        val backoff = ExponentialBackoff(initial = 100.milliseconds, max = 10.seconds, multiplier = 2.0)
        backoff.delayFor(1) shouldBe 100.milliseconds
    }

    @Test
    fun `each subsequent attempt doubles the delay`() {
        val backoff = ExponentialBackoff(initial = 100.milliseconds, max = 10.seconds, multiplier = 2.0)
        backoff.delayFor(2) shouldBe 200.milliseconds
        backoff.delayFor(3) shouldBe 400.milliseconds
        backoff.delayFor(4) shouldBe 800.milliseconds
    }

    @Test
    fun `delay is capped at the configured max`() {
        val backoff = ExponentialBackoff(initial = 1.seconds, max = 5.seconds, multiplier = 2.0)
        backoff.delayFor(10) shouldBe 5.seconds
    }

    @Test
    fun `rejects a non-positive attempt number`() {
        val backoff = ExponentialBackoff()
        try {
            backoff.delayFor(0)
            error("expected IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            // expected
        }
    }
}
