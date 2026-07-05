package indexer.ingestionws.subscriptions

import indexer.schema.SubscriptionRecord

/**
 * The `status` query param this reader can request. Distinct from
 * [indexer.schema.SubscriptionStatus] (which only has ACTIVE/REMOVED as a
 * record's own status) because the API additionally accepts `ALL` as a
 * query-time filter meaning "don't filter on status at all".
 */
enum class SubscriptionStatusFilter { ACTIVE, REMOVED, ALL }

/**
 * Small, swappable interface fetching the currently active subscribed
 * contracts. Kept minimal on purpose per the task brief: the real HTTP
 * implementation below is easy to point at the real subscription-api once it
 * exists; tests use a WireMock stub (component test) or a plain fake
 * (orchestration-level unit test).
 *
 * Shared contract (owned by the subscription-api module, another agent is
 * building it against this exact same contract right now):
 *   GET {baseUrl}/subscriptions?network={network}&status={status}
 *   - network: optional, omit for all networks.
 *   - status: optional, one of ACTIVE|REMOVED|ALL, defaults to ACTIVE.
 *   - 200 OK, JSON array of [SubscriptionRecord].
 */
interface SubscriptionsReader {
    suspend fun fetchSubscriptions(
        network: String? = null,
        status: SubscriptionStatusFilter = SubscriptionStatusFilter.ACTIVE,
    ): List<SubscriptionRecord>
}
