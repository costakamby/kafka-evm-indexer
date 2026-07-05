package indexer.ingestionws.rpc

import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord

/**
 * Pure mapping from the wire-format [RawLogDto] to the schema's
 * [RawLogRecord], tagging [network] and [source] explicitly every time -
 * used identically by the live WS notification path and the reconnect
 * catch-up (`eth_getLogs`) path, so both are guaranteed to tag correctly
 * (acceptance criterion 4.2: "tag every raw log with its source and network").
 */
fun RawLogDto.toRawLogRecord(
    network: String,
    source: IngestionSource,
    observedAtEpochMillis: Long,
): RawLogRecord = RawLogRecord(
    network = network,
    contractAddress = address,
    txHash = transactionHash,
    logIndex = HexCodec.toLong(logIndex),
    blockNumber = HexCodec.toLong(blockNumber),
    blockHash = blockHash,
    topics = topics,
    data = data,
    source = source,
    observedAtEpochMillis = observedAtEpochMillis,
)
