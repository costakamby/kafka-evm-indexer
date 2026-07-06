package indexer.streamstopology.lifecycle

import indexer.schema.ConfirmationStatus
import indexer.schema.DecodedEventEnvelope
import indexer.schema.EventKey
import indexer.schema.ReconciliationAnomaly
import indexer.schema.ReconciliationAnomalyType
import indexer.schema.ktable.ConfirmationState
import indexer.schema.ktable.ReconciliationState
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore

/**
 * Merges ws/poll sightings of the same event (identified by
 * [indexer.schema.EventKey]) into exactly one reconciliation-store entry
 * (acceptance criterion 4.4), flags DIVERGENT_DECODE immediately when a
 * corroborating sighting disagrees with the first, and seeds/refreshes the
 * confirmation-store entry that [BlockTrackingProcessor]'s punctuator later
 * promotes or invalidates.
 *
 * The incoming record's KEY is "network" (this processor's input is an
 * internal, network-partitioned repartition of decoded-logs-topic - see
 * IndexerTopology's "decoded-logs-by-network" step - so it co-locates with
 * BLOCK_TRACKING/CONFIRMATION on the same Streams task), NOT [indexer.schema.EventKey].
 * The real per-event identity is recovered from the record's VALUE
 * (network/txHash/logIndex) and used as the actual RECONCILIATION/CONFIRMATION
 * store key, matching every other consumer of those stores.
 *
 * Retention (4.4's "must not grow unbounded" requirement): a reconciliation-store
 * entry is removed the moment its purpose is served - either both sources have
 * now corroborated (merge complete, confirmation-store carries the event
 * forward) or an anomaly was just flagged for it (by
 * [BlockTrackingProcessor]'s punctuator, see there for the remaining one-sided
 * expiry path). No entry survives past one of those two events.
 */
class ReconciliationProcessor : Processor<String, DecodedEventEnvelope, String, LifecycleOutput> {

    private lateinit var context: ProcessorContext<String, LifecycleOutput>
    private lateinit var reconciliationStore: KeyValueStore<String, ReconciliationState>
    private lateinit var confirmationStore: KeyValueStore<String, ConfirmationState>

    override fun init(context: ProcessorContext<String, LifecycleOutput>) {
        this.context = context
        reconciliationStore = context.getStateStore(StoreNames.RECONCILIATION)
        confirmationStore = context.getStateStore(StoreNames.CONFIRMATION)
    }

    override fun process(record: Record<String, DecodedEventEnvelope>) {
        val incoming = record.value()
        val key = EventKey.of(incoming.network, incoming.txHash, incoming.logIndex)

        val existing = reconciliationStore.get(key)
        val result = ReconciliationLogic.merge(existing, incoming, context.currentSystemTimeMs())

        if (result.divergent) {
            val anomaly = ReconciliationAnomaly(
                type = ReconciliationAnomalyType.DIVERGENT_DECODE,
                network = incoming.network,
                txHash = incoming.txHash,
                logIndex = incoming.logIndex,
                detectedAtEpochMillis = context.currentSystemTimeMs(),
                details = "decoded fields differ between the first and corroborating sighting for this event",
            )
            context.forward(Record(key, LifecycleOutput.Anomaly(anomaly), context.currentSystemTimeMs()))
        }

        if (result.state.seenWs && result.state.seenPoll) {
            // Merge complete: reconciliation's job is done. Removing here (rather
            // than letting it linger) is the primary bound on this store's growth.
            reconciliationStore.delete(key)
        } else {
            reconciliationStore.put(key, result.state)
        }

        seedOrRefreshConfirmation(key, incoming, result.state.decodedEvent ?: incoming)
    }

    private fun seedOrRefreshConfirmation(key: String, incoming: DecodedEventEnvelope, canonicalDecoded: DecodedEventEnvelope) {
        val existingConfirmation = confirmationStore.get(key)
        when {
            existingConfirmation == null -> confirmationStore.put(
                key,
                ConfirmationState(
                    network = incoming.network,
                    txHash = incoming.txHash,
                    logIndex = incoming.logIndex,
                    status = ConfirmationStatus.UNCONFIRMED,
                    confirmationsSeen = 0,
                    decodedEvent = canonicalDecoded,
                ),
            )
            existingConfirmation.status == ConfirmationStatus.UNCONFIRMED -> confirmationStore.put(
                key,
                existingConfirmation.copy(decodedEvent = canonicalDecoded),
            )
            // else: already CONFIRMED or INVALIDATED (terminal) - a late/duplicate
            // sighting must never resurrect or downgrade a terminal outcome.
        }
    }

    override fun close() {}
}
