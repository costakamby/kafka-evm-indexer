package indexer.subscriptionapi.subscriptions

import indexer.decoder.AbiRegistry
import indexer.schema.SubscriptionRecord
import indexer.schema.SubscriptionStatus
import indexer.subscriptionapi.module
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.setBody
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * Ktor route tests (test pyramid layer 1-ish: Ktor's in-process test host, no
 * real Kafka) for the subscription REST API contract (design doc 4.1), using
 * fakes for the writer/reader so these tests exercise only route/validation
 * logic - GlobalKTable convergence itself is proven separately in
 * SubscriptionConvergenceTest.
 */
class SubscriptionRoutesTest {

    private fun deps(writer: FakeSubscriptionWriter = FakeSubscriptionWriter(), reader: FakeSubscriptionReader = FakeSubscriptionReader()) =
        AppDependencies(
            subscriptionWriter = writer,
            subscriptionReader = reader,
            abiRegistry = AbiRegistry(),
        )

    @Test
    fun `POST subscriptions with a valid abiRef publishes an ACTIVE record and responds 201`() = testApplication {
        val writer = FakeSubscriptionWriter()
        application { module(); subscriptionModule(deps(writer)) }

        val response = client.post("/subscriptions") {
            contentType(ContentType.Application.Json)
            setBody(
                """{"network":"ethereum","address":"0xdead","abiRef":"erc20","startBlock":null,"includeEvents":["Transfer"]}""",
            )
        }

        response.status shouldBe HttpStatusCode.Created
        writer.published.size shouldBe 1
        val published = writer.published.single()
        published.status shouldBe SubscriptionStatus.ACTIVE
        published.network shouldBe "ethereum"
        published.address shouldBe "0xdead"
        published.abiRef shouldBe "erc20"
        published.id.isNotBlank() shouldBe true

        val body = Json.parseToJsonElement(response.bodyAsText())
        body.toString() shouldContain published.id
    }

    @Test
    fun `subscribingWithInvalidAbi_isRejectedAtApiLayer`() = testApplication {
        val writer = FakeSubscriptionWriter()
        application { module(); subscriptionModule(deps(writer)) }

        val response = client.post("/subscriptions") {
            contentType(ContentType.Application.Json)
            setBody(
                """{"network":"ethereum","address":"0xdead","abiRef":"no-such-abi","startBlock":null,"includeEvents":[]}""",
            )
        }

        response.status shouldBe HttpStatusCode.BadRequest
        // Nothing must be produced to subscriptions-topic before validation passes.
        writer.published.shouldBeEmpty()
        response.bodyAsText() shouldContain "abiRef"
    }

    @Test
    fun `DELETE subscriptions by id publishes a REMOVED record for the same id and responds 204`() = testApplication {
        val writer = FakeSubscriptionWriter()
        val reader = FakeSubscriptionReader()
        val existing = SubscriptionRecord(
            id = "sub-1",
            network = "ethereum",
            address = "0xdead",
            abiRef = "erc20",
            status = SubscriptionStatus.ACTIVE,
            createdAtEpochMillis = 1,
        )
        reader.put(existing)
        application { module(); subscriptionModule(deps(writer, reader)) }

        val response = client.delete("/subscriptions/sub-1")

        response.status shouldBe HttpStatusCode.NoContent
        writer.published.size shouldBe 1
        val removed = writer.published.single()
        removed.id shouldBe "sub-1"
        removed.status shouldBe SubscriptionStatus.REMOVED
    }

    @Test
    fun `DELETE subscriptions for an unknown id responds 404 and publishes nothing`() = testApplication {
        val writer = FakeSubscriptionWriter()
        application { module(); subscriptionModule(deps(writer)) }

        val response = client.delete("/subscriptions/does-not-exist")

        response.status shouldBe HttpStatusCode.NotFound
        writer.published.shouldBeEmpty()
    }

    @Test
    fun `GET subscriptions defaults to ACTIVE status when status is omitted`() = testApplication {
        val reader = FakeSubscriptionReader()
        reader.put(activeSub("a", "ethereum"))
        reader.put(removedSub("b", "ethereum"))
        application { module(); subscriptionModule(deps(reader = reader)) }

        val response = client.get("/subscriptions")

        response.status shouldBe HttpStatusCode.OK
        val ids = Json.parseToJsonElement(response.bodyAsText()).toString()
        ids shouldContain "\"a\""
        (ids.contains("\"b\"")) shouldBe false
    }

    @Test
    fun `GET subscriptions filters by network and honors status=ALL`() = testApplication {
        val reader = FakeSubscriptionReader()
        reader.put(activeSub("a", "ethereum"))
        reader.put(removedSub("b", "ethereum"))
        reader.put(activeSub("c", "polygon"))
        application { module(); subscriptionModule(deps(reader = reader)) }

        val response = client.get("/subscriptions?network=ethereum&status=ALL")

        response.status shouldBe HttpStatusCode.OK
        val body = response.bodyAsText()
        body shouldContain "\"a\""
        body shouldContain "\"b\""
        (body.contains("\"c\"")) shouldBe false
    }

    private fun activeSub(id: String, network: String) = SubscriptionRecord(
        id = id, network = network, address = "0xaddr-$id", abiRef = "erc20",
        status = SubscriptionStatus.ACTIVE, createdAtEpochMillis = 1,
    )

    private fun removedSub(id: String, network: String) = SubscriptionRecord(
        id = id, network = network, address = "0xaddr-$id", abiRef = "erc20",
        status = SubscriptionStatus.REMOVED, createdAtEpochMillis = 1,
    )
}
