package indexer.integrationtests.fixtures

import org.web3j.protocol.Web3j

/**
 * Reusable reorg simulation helper (design doc section 5.3): snapshot,
 * mine block N with one action, revert, then mine an alternate block N
 * with a different action. Built once here for reuse by both the
 * confirmation-lifecycle tests (4.5) and HA/failover tests (4.6).
 */
class ReorgFixture(private val web3j: Web3j, private val rpc: AnvilRpcClient) {

    data class ReorgResult(
        val originalTxHash: String,
        val originalBlockHash: String,
        val replacementTxHash: String,
        val replacementBlockHash: String,
    )

    /**
     * Snapshots current state, runs [originalAction] to mine a block,
     * reverts to the snapshot, then runs [replacementAction] to mine a
     * different block at the same height. Both actions must return the
     * hash of the transaction they caused to be mined.
     */
    fun simulateReorg(originalAction: () -> String, replacementAction: () -> String): ReorgResult {
        val snapshotId = rpc.snapshot()

        val originalTxHash = originalAction()
        val originalReceipt = web3j.ethGetTransactionReceipt(originalTxHash).send().transactionReceipt.get()

        check(rpc.revert(snapshotId)) { "evm_revert to snapshot $snapshotId failed" }

        val replacementTxHash = replacementAction()
        val replacementReceipt = web3j.ethGetTransactionReceipt(replacementTxHash).send().transactionReceipt.get()

        return ReorgResult(
            originalTxHash = originalTxHash,
            originalBlockHash = originalReceipt.blockHash,
            replacementTxHash = replacementTxHash,
            replacementBlockHash = replacementReceipt.blockHash,
        )
    }
}
