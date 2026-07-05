package indexer.ingestionws.ws

/**
 * Pure computation of the exact `eth_getLogs` catch-up range for a WS
 * reconnect (design decision 7 / acceptance criterion 4.2: "issues an
 * eth_getLogs catch-up call for the exact gap range"). Never trusts WS to
 * have replayed anything - the range is derived purely from the last block
 * number this listener actually observed and the chain's current head.
 */
object CatchupRange {
    /**
     * @param lastSeenBlock the last block number this listener has observed
     *   for this network, or null if it has never established a baseline yet
     *   (nothing to catch up - live streaming starts fresh from head).
     * @param currentHead the chain head as of right now (`eth_blockNumber`).
     * @return the inclusive range to fetch via `eth_getLogs`, or null if
     *   there is no gap to fill.
     */
    fun compute(lastSeenBlock: Long?, currentHead: Long): LongRange? {
        if (lastSeenBlock == null) return null
        val from = lastSeenBlock + 1
        if (from > currentHead) return null
        return from..currentHead
    }
}
