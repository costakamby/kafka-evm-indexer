package indexer.ingestionws.rpc

/**
 * Small, swappable interface for the two JSON-RPC calls this module needs
 * from a plain HTTPS RPC endpoint (never the WS endpoint - catch-up must
 * never depend on the socket that just disconnected). Kept as an interface
 * so orchestration logic ([indexer.ingestionws.ws.WsIngestionRunner]) can be
 * proven fast and deterministically against a fake, independent of real IO.
 */
interface EthRpcClient {
    suspend fun ethBlockNumber(): Long

    suspend fun ethGetLogs(fromBlock: Long, toBlock: Long, addresses: List<String>): List<RawLogDto>
}
