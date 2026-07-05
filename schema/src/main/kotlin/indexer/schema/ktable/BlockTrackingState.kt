package indexer.schema.ktable

import kotlinx.serialization.Serializable

@Serializable
data class BlockHashEntry(
    val blockNumber: Long,
    val blockHash: String,
)

/**
 * Block-tracking KTable value, keyed by network (design doc section 2).
 * [recentBlockHashes] holds enough ancestry to detect reorgs up to the
 * configured confirmation depth (acceptance criteria 4.5).
 */
@Serializable
data class BlockTrackingState(
    val network: String,
    val lastBlock: Long,
    val recentBlockHashes: List<BlockHashEntry>,
    val reorgDepthWatermark: Int,
)
