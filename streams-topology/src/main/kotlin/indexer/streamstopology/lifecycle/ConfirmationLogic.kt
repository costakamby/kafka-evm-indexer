package indexer.streamstopology.lifecycle

/**
 * Pure arithmetic driving the UNCONFIRMED -> CONFIRMED transition off
 * block-tracking state (never wall-clock time) - acceptance criterion 4.5.
 */
object ConfirmationLogic {
    fun confirmationsSeen(lastBlock: Long, eventBlock: Long): Int =
        (lastBlock - eventBlock).coerceAtLeast(0).let {
            if (it > Int.MAX_VALUE) Int.MAX_VALUE else it.toInt()
        }

    fun shouldConfirm(confirmationsSeen: Int, depth: Int): Boolean = confirmationsSeen >= depth
}
