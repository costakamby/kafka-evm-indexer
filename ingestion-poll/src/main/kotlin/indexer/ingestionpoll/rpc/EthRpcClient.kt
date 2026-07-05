package indexer.ingestionpoll.rpc

/** A single log entry as returned by eth_getLogs, before mapping to the schema's RawLogRecord. */
data class RawLog(
    val address: String,
    val topics: List<String>,
    val data: String,
    val blockNumber: Long,
    val blockHash: String,
    val transactionHash: String,
    val logIndex: Long,
)

/** A non-retryable JSON-RPC error (anything that isn't a rate-limit signal). */
class RpcErrorException(message: String, val code: Int? = null, cause: Throwable? = null) :
    Exception(message, cause)

/**
 * Raised only after [maxRetries] rate-limited attempts are exhausted for one
 * specific block range. The range is surfaced on the exception explicitly so
 * callers can see - and never silently drop - exactly what was being
 * attempted (design doc 4.2: "without dropping the range it was attempting").
 */
class RateLimitExceededException(val fromBlock: Long, val toBlock: Long, cause: Throwable?) :
    Exception("rate limit retries exhausted for block range [$fromBlock, $toBlock]", cause)

/** Minimal JSON-RPC surface this module needs against an EVM node. */
interface EthRpcClient {
    suspend fun blockNumber(): Long

    suspend fun getLogs(fromBlock: Long, toBlock: Long, addresses: List<String>): List<RawLog>
}
