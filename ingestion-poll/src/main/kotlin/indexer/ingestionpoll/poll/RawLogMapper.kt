package indexer.ingestionpoll.poll

import indexer.ingestionpoll.rpc.RawLog
import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord

/**
 * Maps the RPC layer's [RawLog] into the shared schema's [RawLogRecord],
 * always tagging source=POLL (design doc 4.2: "Both ingestion paths
 * correctly tag every raw log with its source ... and network" - the ws
 * side is a different module's job).
 */
object RawLogMapper {
    fun toRawLogRecord(
        network: String,
        log: RawLog,
        observedAtEpochMillis: Long = System.currentTimeMillis(),
    ): RawLogRecord =
        RawLogRecord(
            network = network,
            contractAddress = log.address,
            txHash = log.transactionHash,
            logIndex = log.logIndex,
            blockNumber = log.blockNumber,
            blockHash = log.blockHash,
            topics = log.topics,
            data = log.data,
            source = IngestionSource.POLL,
            observedAtEpochMillis = observedAtEpochMillis,
        )
}
