package indexer.ingestionws.ws

import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Pure exponential backoff arithmetic for WS reconnect attempts (design
 * decision 7). `attempt` is 1-indexed (the first reconnect attempt).
 */
class ExponentialBackoff(
    private val initial: Duration = 500.milliseconds,
    private val max: Duration = 30.seconds,
    private val multiplier: Double = 2.0,
) {
    fun delayFor(attempt: Int): Duration {
        require(attempt >= 1) { "attempt must be >= 1, was $attempt" }
        val scaled = initial * multiplier.pow(attempt - 1)
        return if (scaled > max) max else scaled
    }
}
