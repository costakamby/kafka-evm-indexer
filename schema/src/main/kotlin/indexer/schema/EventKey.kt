package indexer.schema

/**
 * The shared reconciliation/confirmation key (network, tx_hash, log_index)
 * from design doc decision 3, rendered as a stable Kafka record key.
 */
object EventKey {
    fun of(network: String, txHash: String, logIndex: Long): String = "$network:$txHash:$logIndex"
}
