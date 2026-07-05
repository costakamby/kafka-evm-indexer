package indexer.subscriptionapi

import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.Test

/**
 * Ktor's official ktor-server-metrics-micrometer plugin, wired to a
 * PrometheusMeterRegistry, exposed on /metrics (design doc Phase 0 step 4).
 */
class MetricsEndpointTest {

    @Test
    fun `GET slash metrics returns Prometheus-format output`() {
        testApplication {
            application { module() }

            val response = client.get("/metrics")

            response.status shouldBe HttpStatusCode.OK
            val body = response.bodyAsText()
            body shouldContain "# TYPE"
            body shouldContain "# HELP"
        }
    }
}
