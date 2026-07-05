package indexer.subscriptionapi

import indexer.subscriptionapi.config.AppConfigLoader
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import kotlinx.serialization.json.Json

fun main() {
    val config = AppConfigLoader.load()
    embeddedServer(Netty, port = config.server.port, module = Application::module).start(wait = true)
}

fun Application.module() {
    val prometheusRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    JvmMemoryMetrics().bindTo(prometheusRegistry)
    JvmGcMetrics().bindTo(prometheusRegistry)
    JvmThreadMetrics().bindTo(prometheusRegistry)
    ProcessorMetrics().bindTo(prometheusRegistry)

    install(MicrometerMetrics) {
        registry = prometheusRegistry
    }
    install(ContentNegotiation) {
        json(Json { ignoreUnknownKeys = true })
    }
    install(CallLogging)

    routing {
        get("/health") {
            call.respond(mapOf("status" to "ok"))
        }
        get("/metrics") {
            call.respond(prometheusRegistry.scrape())
        }
    }
}
