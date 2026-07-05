package indexer.ingestionpoll.rpc

/**
 * Exponential backoff for provider rate-limit retries (design doc 4.2). Pure
 * arithmetic, deliberately kept free of any sleeping/coroutine concerns so it
 * is trivially unit-testable.
 */
object Backoff {
    /** attempt is 0-based: delay before the (attempt+1)-th retry. */
    fun delayMillis(attempt: Int, initialDelayMs: Long, maxDelayMs: Long): Long {
        require(attempt >= 0) { "attempt must be >= 0, was $attempt" }
        require(initialDelayMs >= 0) { "initialDelayMs must be >= 0, was $initialDelayMs" }
        require(maxDelayMs >= 0) { "maxDelayMs must be >= 0, was $maxDelayMs" }
        // Cap the shift well below 63: initialDelayMs (ms, realistically well under a
        // minute) * 2^40 is already ~1e12 times the initial delay, comfortably past any
        // sane maxDelayMs, while staying far from overflowing a Long.
        val shift = attempt.coerceAtMost(40)
        val scaled = initialDelayMs * (1L shl shift)
        return if (scaled < 0 || scaled > maxDelayMs) maxDelayMs else scaled
    }
}
