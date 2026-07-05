package indexer.subscriptionapi.subscriptions

import indexer.schema.SubscriptionRecord
import indexer.schema.SubscriptionStatus
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import java.util.UUID

/**
 * REST routes for design doc 4.1's subscription management contract, wired to
 * whatever [AppDependencies] are supplied - production wiring builds real
 * Kafka-backed writer/reader in main(); tests inject fakes. Deliberately does
 * NOT re-install ContentNegotiation/CallLogging - callers compose this
 * alongside the existing `module()` (Phase 0), which already installs those.
 */
fun Application.subscriptionModule(deps: AppDependencies) {
    routing {
        post("/subscriptions") {
            val request = call.receive<CreateSubscriptionRequest>()

            if (!deps.abiRegistry.isValid(request.abiRef)) {
                call.respond(
                    HttpStatusCode.BadRequest,
                    ErrorResponse("abiRef '${request.abiRef}' does not resolve to a valid, parseable ABI"),
                )
                return@post
            }

            val record = SubscriptionRecord(
                id = UUID.randomUUID().toString(),
                network = request.network,
                address = request.address,
                abiRef = request.abiRef,
                startBlock = request.startBlock,
                includeEvents = request.includeEvents,
                status = SubscriptionStatus.ACTIVE,
                createdAtEpochMillis = System.currentTimeMillis(),
            )
            // Validation happens BEFORE anything is produced (acceptance criterion 4.1).
            deps.subscriptionWriter.publish(record)
            call.respond(HttpStatusCode.Created, record)
        }

        delete("/subscriptions/{id}") {
            val id = call.parameters["id"]!!
            val existing = deps.subscriptionReader.all().find { it.id == id }
            if (existing == null) {
                call.respond(HttpStatusCode.NotFound, ErrorResponse("no subscription with id '$id'"))
                return@delete
            }

            // Same key (id) so compaction retains only the latest status - this is
            // how ws/poll's periodic ACTIVE poll naturally stops returning it.
            deps.subscriptionWriter.publish(existing.copy(status = SubscriptionStatus.REMOVED))
            call.respond(HttpStatusCode.NoContent)
        }

        get("/subscriptions") {
            val networkFilter = call.request.queryParameters["network"]
            val statusFilter = call.request.queryParameters["status"]?.uppercase() ?: "ACTIVE"

            val all = deps.subscriptionReader.all()
            val filtered = all
                .filter { networkFilter == null || it.network == networkFilter }
                .filter { statusFilter == "ALL" || it.status.name == statusFilter }

            call.respond(HttpStatusCode.OK, filtered)
        }
    }
}
