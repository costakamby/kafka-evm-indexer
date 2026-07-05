package indexer.ingestionpoll.poll

import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.junit5.WireMockExtension
import indexer.ingestionpoll.progress.KafkaPollProgressStore
import indexer.ingestionpoll.rpc.HttpEthRpcClient
import indexer.schema.IngestionSource
import indexer.schema.RawLogRecord
import indexer.schema.SubscriptionRecord
import indexer.schema.SubscriptionStatus
import indexer.schema.json.IndexerJson
import io.kotest.matchers.shouldBe
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 * Component test (design doc 5.2 layer 3): ContractPoller's actual
 * orchestration - eth_getLogs calls against a WireMock-stubbed RPC endpoint,
 * emitting to a MockProducer standing in for raw-logs-topic, watermark reads
 * from an in-memory-backed KafkaPollProgressStore (MockProducer, no live
 * broker). Covers design doc 4.2: batch-size/block-range respect, source and
 * network tagging, and historical backfill for a new subscription with a
 * start_block in the past.
 */
class ContractPollerTest {

    companion object {
        @JvmField
        @RegisterExtension
        val wireMock: WireMockExtension = WireMockExtension.newInstance().build()
    }

    private lateinit var rawLogsProducer: MockProducer<String, String>
    private lateinit var progressStore: KafkaPollProgressStore
    private lateinit var poller: ContractPoller

    @BeforeEach
    fun setUp() {
        rawLogsProducer = MockProducer(true, StringSerializer(), StringSerializer())
        progressStore = KafkaPollProgressStore(producer = MockProducer(true, StringSerializer(), StringSerializer()))
        val rpcClient = HttpEthRpcClient(
            httpClient = HttpClient(CIO),
            rpcUrl = wireMock.baseUrl(),
            maxRetries = 3,
            initialBackoffMs = 1,
            maxBackoffMs = 5,
        )
        poller = ContractPoller(
            rpcClient = rpcClient,
            progressStore = progressStore,
            producer = rawLogsProducer,
            rawLogsTopic = "raw-logs-topic",
            maxBlockRange = 50,
        )
    }

    private fun stubBlockNumber(head: Long) {
        wireMock.stubFor(
            post(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$[?(@.method == 'eth_blockNumber')]"))
                .willReturn(aResponse().withStatus(200).withBody("""{"jsonrpc":"2.0","id":1,"result":"${HttpEthRpcClient.longToHex(head)}"}""")),
        )
    }

    private fun stubLogsForRange(fromBlock: Long, toBlock: Long, logs: String = "[]") {
        wireMock.stubFor(
            post(urlEqualTo("/"))
                .withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]"))
                .withRequestBody(matchingJsonPath("$.params[0][?(@.fromBlock == '${HttpEthRpcClient.longToHex(fromBlock)}')]"))
                .withRequestBody(matchingJsonPath("$.params[0][?(@.toBlock == '${HttpEthRpcClient.longToHex(toBlock)}')]"))
                .willReturn(aResponse().withStatus(200).withBody("""{"jsonrpc":"2.0","id":1,"result":$logs}""")),
        )
    }

    private fun sub(startBlock: Long?, address: String = "0xabc") = SubscriptionRecord(
        id = "sub-1",
        network = "ethereum",
        address = address,
        abiRef = "erc20-v1",
        startBlock = startBlock,
        includeEvents = listOf("Transfer"),
        status = SubscriptionStatus.ACTIVE,
        createdAtEpochMillis = 0,
    )

    @Test
    fun `a brand-new subscription with a start_block in the past triggers a chunked historical backfill to head`() {
        stubBlockNumber(head = 120)
        stubLogsForRange(0, 49)
        stubLogsForRange(50, 99)
        stubLogsForRange(
            100, 120,
            logs = """
                [{"address":"0xabc","topics":["0xT0"],"data":"0x01","blockNumber":"0x64",
                  "blockHash":"0xbh","transactionHash":"0xth","logIndex":"0x0"}]
            """.trimIndent(),
        )

        runBlocking { poller.pollNetwork("ethereum", listOf(sub(startBlock = 0))) }

        // no range was skipped: all three chunks from 0 to head=120 were requested.
        wireMock.verify(postRequestedFor(urlEqualTo("/")).withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]")))
        val sent = rawLogsProducer.history()
        sent.size shouldBe 1
        sent.single().topic() shouldBe "raw-logs-topic"
        val record = IndexerJson.instance.decodeFromString<RawLogRecord>(sent.single().value())
        record.network shouldBe "ethereum"
        record.contractAddress shouldBe "0xabc"
        record.source shouldBe IngestionSource.POLL
        record.blockNumber shouldBe 100L

        progressStore.lastPolledBlock("ethereum", "0xabc") shouldBe 120L
    }

    @Test
    fun `after backfilling, a later poll cycle continues incrementally from head plus one without re-backfilling`() {
        stubBlockNumber(head = 100)
        stubLogsForRange(0, 49)
        stubLogsForRange(50, 99)
        stubLogsForRange(100, 100)

        runBlocking { poller.pollNetwork("ethereum", listOf(sub(startBlock = 0))) }
        progressStore.lastPolledBlock("ethereum", "0xabc") shouldBe 100L

        // chain advances to a new head; only the new range [101, 110] should ever be requested now.
        wireMock.resetAll()
        stubBlockNumber(head = 110)
        stubLogsForRange(101, 110)

        runBlocking { poller.pollNetwork("ethereum", listOf(sub(startBlock = 0))) }

        progressStore.lastPolledBlock("ethereum", "0xabc") shouldBe 110L
        wireMock.verify(
            com.github.tomakehurst.wiremock.client.WireMock.exactly(1),
            postRequestedFor(urlEqualTo("/")).withRequestBody(matchingJsonPath("$[?(@.method == 'eth_getLogs')]")),
        )
    }

    @Test
    fun `two contracts on the same network track independent watermarks`() {
        stubBlockNumber(head = 60)
        stubLogsForRange(60, 60) // contract A: brand new, no start_block set -> starts from the head, single-block range
        stubLogsForRange(30, 60) // contract B: start_block = 30

        val subs = listOf(
            sub(startBlock = null, address = "0xaaa"),
            sub(startBlock = 30, address = "0xbbb"),
        )

        runBlocking { poller.pollNetwork("ethereum", subs) }

        // contract A had no start_block, so it starts from the head (nothing before it to backfill)
        progressStore.lastPolledBlock("ethereum", "0xaaa") shouldBe 60L
        progressStore.lastPolledBlock("ethereum", "0xbbb") shouldBe 60L
    }
}
