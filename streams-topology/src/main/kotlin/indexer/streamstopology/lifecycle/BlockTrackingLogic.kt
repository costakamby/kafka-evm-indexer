package indexer.streamstopology.lifecycle

import indexer.schema.ktable.BlockHashEntry
import indexer.schema.ktable.BlockTrackingState

data class BlockTrackingUpdateResult(
    val state: BlockTrackingState,
    /** Non-null when this update replaced a previously-seen hash at a height (a reorg). */
    val reorgAtHeight: Long?,
)

/**
 * Pure block-hash ancestry maintenance (acceptance criterion 4.5): appends new
 * (blockNumber, blockHash) observations, detects a reorg when a height that was
 * already observed reappears with a DIFFERENT hash, and keeps the ancestry
 * window bounded so the block-tracking KTable does not grow unboundedly.
 *
 * Reorg handling here is intentionally height-granular, not per-transaction:
 * on a detected mismatch at height H, ancestry above H is dropped (it belonged
 * to the stale fork) and [BlockTrackingState.lastBlock] rolls back to H: later
 * sightings naturally push it forward again. See streams-topology's
 * `ReorgInvalidationTest`/report notes for why confirmation-state invalidation
 * keys off "was this event's block height reorged" rather than a per-event
 * block-hash comparison (schema doesn't carry blockHash on decoded/confirmation
 * records, only on RawLogRecord) - a deliberate, disclosed simplification, not
 * a bug.
 */
object BlockTrackingLogic {

    /** How many extra blocks of ancestry to retain beyond the confirmation depth. */
    private const val RETENTION_BUFFER = 10

    fun applyNewBlock(
        current: BlockTrackingState?,
        network: String,
        blockNumber: Long,
        blockHash: String,
        depth: Int,
    ): BlockTrackingUpdateResult {
        if (current == null) {
            return BlockTrackingUpdateResult(
                state = BlockTrackingState(
                    network = network,
                    lastBlock = blockNumber,
                    recentBlockHashes = listOf(BlockHashEntry(blockNumber, blockHash)),
                    reorgDepthWatermark = depth,
                ),
                reorgAtHeight = null,
            )
        }

        val existingEntry = current.recentBlockHashes.find { it.blockNumber == blockNumber }

        if (existingEntry != null && existingEntry.blockHash == blockHash) {
            // Duplicate observation: no-op.
            return BlockTrackingUpdateResult(current, reorgAtHeight = null)
        }

        val retentionFloor = maxOf(current.lastBlock, blockNumber) - (depth + RETENTION_BUFFER)

        if (existingEntry != null) {
            // Reorg: height `blockNumber` now has a different canonical hash.
            // Drop ancestry above it (stale fork) and roll lastBlock back to it.
            val trimmed = current.recentBlockHashes
                .filter { it.blockNumber < blockNumber && it.blockNumber > retentionFloor }
                .plus(BlockHashEntry(blockNumber, blockHash))
                .sortedBy { it.blockNumber }
            return BlockTrackingUpdateResult(
                state = current.copy(
                    lastBlock = blockNumber,
                    recentBlockHashes = trimmed,
                    reorgDepthWatermark = depth,
                ),
                reorgAtHeight = blockNumber,
            )
        }

        // Brand-new height: append and trim to the retention window.
        val newLastBlock = maxOf(current.lastBlock, blockNumber)
        val newFloor = newLastBlock - (depth + RETENTION_BUFFER)
        val extended = (current.recentBlockHashes + BlockHashEntry(blockNumber, blockHash))
            .filter { it.blockNumber > newFloor }
            .sortedBy { it.blockNumber }

        return BlockTrackingUpdateResult(
            state = current.copy(
                lastBlock = newLastBlock,
                recentBlockHashes = extended,
                reorgDepthWatermark = depth,
            ),
            reorgAtHeight = null,
        )
    }
}
