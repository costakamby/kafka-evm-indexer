package indexer.ingestionws.ws

import indexer.ingestionws.rpc.RawLogDto

/**
 * Small, swappable abstraction over "connect to the node's WS endpoint,
 * eth_subscribe to logs for these addresses, and stream notifications until
 * the connection ends". Kept as an interface so [WsIngestionRunner]'s
 * reconnect/catch-up orchestration can be proven fast and deterministically
 * against a fake, independent of real sockets (see WsIngestionRunnerTest).
 * The real ktor-based implementation is proven separately at the IO layer
 * against a local fake WS server (see the component test).
 *
 * Contract: [connectAndStream] suspends for the lifetime of the connection,
 * invoking [onLog] for every log notification received. It returns normally
 * when the connection closes cleanly, or throws if it fails - either way the
 * caller treats "the suspend function returned/threw" as "disconnected,
 * time to reconnect" (a CancellationException always propagates as normal
 * structured-concurrency shutdown, never as "disconnected").
 */
interface WsTransport {
    suspend fun connectAndStream(addresses: List<String>, onLog: suspend (RawLogDto) -> Unit)
}
