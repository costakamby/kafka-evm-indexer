package indexer.streamstopology.config

/**
 * Per-network parameters the topology needs at build time (design decision 11).
 * Deliberately decoupled from subscription-api's Hoplite-loaded `AppConfig` /
 * `NetworkConfig` so this module has zero config-library dependency - the host
 * app (subscription-api) is responsible for mapping its own config into this
 * shape when it builds the topology.
 *
 * [reconciliationGapBlocks] is this module's own knob (not in the design doc's
 * fixed config surface): how many blocks may elapse before a one-sided sighting
 * (poll-only or ws-only) is flagged as a reconciliation anomaly (4.4). Kept
 * small and independent of [confirmationDepth] so tests can exercise anomaly
 * detection without waiting out a full confirmation window.
 */
data class NetworkParams(
    val confirmationDepth: Int,
    val reconciliationGapBlocks: Int = 3,
)

data class NetworkTopologyConfig(val networks: Map<String, NetworkParams>) {
    fun forNetwork(network: String): NetworkParams =
        networks[network] ?: error("no NetworkParams configured for network '$network'")
}
