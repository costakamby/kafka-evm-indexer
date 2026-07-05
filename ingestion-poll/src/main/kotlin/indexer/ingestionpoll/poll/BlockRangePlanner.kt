package indexer.ingestionpoll.poll

/** An inclusive [fromBlock, toBlock] range for a single eth_getLogs call. */
data class BlockRange(val fromBlock: Long, val toBlock: Long)

/**
 * Pure logic deciding (a) where a contract's next poll should start from -
 * its own watermark, or its subscription's start_block for a brand-new
 * backfill, or the current head if neither is known - and (b) how to chunk
 * a [fromBlock, toBlock] span into calls no larger than maxBlockRange
 * (design doc 4.2: "REST poller respects configurable batch size / block
 * range per call" and "a new subscription with a start_block in the past
 * triggers a correct historical backfill").
 */
object BlockRangePlanner {

    fun plan(fromBlock: Long, toBlock: Long, maxBlockRange: Long): List<BlockRange> {
        require(maxBlockRange > 0) { "maxBlockRange must be positive, was $maxBlockRange" }
        if (fromBlock > toBlock) return emptyList()

        val ranges = mutableListOf<BlockRange>()
        var start = fromBlock
        while (start <= toBlock) {
            val end = minOf(start + maxBlockRange - 1, toBlock)
            ranges += BlockRange(start, end)
            start = end + 1
        }
        return ranges
    }

    /**
     * - Already-tracked contract: resume right after its last successfully polled block.
     * - Brand-new contract with a start_block in the past: backfill from start_block.
     * - Brand-new contract with no start_block: nothing to backfill, start from the head.
     */
    fun resolveStartBlock(lastPolledBlock: Long?, subscriptionStartBlock: Long?, currentHead: Long): Long =
        when {
            lastPolledBlock != null -> lastPolledBlock + 1
            subscriptionStartBlock != null -> subscriptionStartBlock
            else -> currentHead
        }
}
