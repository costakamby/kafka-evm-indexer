package indexer.streamstopology.lifecycle

import indexer.schema.ReconciliationAnomaly
import indexer.schema.ktable.ConfirmationState

/**
 * Common output type forwarded by both [BlockTrackingProcessor] and
 * [ReconciliationProcessor] via the KIP-820 non-void `KStream.process(...)`
 * API, so both processors can feed the same downstream `.split()` into
 * confirmed-events-topic vs reconciliation-anomalies-topic without needing
 * `context.forward` sink wiring inside the Processor API itself.
 */
sealed interface LifecycleOutput {
    data class Confirmation(val state: ConfirmationState) : LifecycleOutput
    data class Anomaly(val anomaly: ReconciliationAnomaly) : LifecycleOutput
}
