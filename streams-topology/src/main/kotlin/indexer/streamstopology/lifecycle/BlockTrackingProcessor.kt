package indexer.streamstopology.lifecycle

import indexer.schema.ConfirmationStatus
import indexer.schema.RawLogRecord
import indexer.schema.ReconciliationAnomaly
import indexer.schema.ktable.BlockTrackingState
import indexer.schema.ktable.ConfirmationState
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import indexer.streamstopology.config.NetworkTopologyConfig
import java.time.Duration

/**
 * Maintains the block-tracking KTable (keyed by network) and drives two
 * time-sensitive pieces of the lifecycle off it:
 *
 * 1. Reorg invalidation (acceptance criterion 4.5) - EVENT-DRIVEN, not
 *    punctuator-driven: the instant a raw log reveals that an
 *    already-observed height now has a different canonical hash, every
 *    still-UNCONFIRMED confirmation-store entry at that (network, height) is
 *    marked INVALIDATED and an explicit correction is forwarded, before any
 *    later punctuator pass could have promoted it to CONFIRMED. Height-level
 *    granularity (not per-tx block-hash comparison) is a deliberate,
 *    documented simplification - see BlockTrackingLogic's kdoc.
 *
 * 2. UNCONFIRMED -> CONFIRMED promotion and reconciliation-anomaly detection
 *    (acceptance criteria 4.4 / 4.5) - PUNCTUATOR-driven via
 *    `PunctuationType.STREAM_TIME` (never wall-clock time), scheduled on this
 *    processor because it is what advances block-tracking: the punctuator
 *    fires as raw-logs-topic's stream time advances, i.e. genuinely "tied to
 *    block-tracking advances".
 */
class BlockTrackingProcessor(
    private val config: NetworkTopologyConfig,
) : Processor<String, RawLogRecord, String, LifecycleOutput> {

    /** Extra blocks beyond confirmationDepth before a terminal confirmation-store entry is swept. */
    private val terminalRetentionBufferBlocks = 5

    private lateinit var context: ProcessorContext<String, LifecycleOutput>
    private lateinit var blockTrackingStore: KeyValueStore<String, BlockTrackingState>
    private lateinit var reconciliationStore: KeyValueStore<String, indexer.schema.ktable.ReconciliationState>
    private lateinit var confirmationStore: KeyValueStore<String, ConfirmationState>

    override fun init(context: ProcessorContext<String, LifecycleOutput>) {
        this.context = context
        blockTrackingStore = context.getStateStore(StoreNames.BLOCK_TRACKING)
        reconciliationStore = context.getStateStore(StoreNames.RECONCILIATION)
        confirmationStore = context.getStateStore(StoreNames.CONFIRMATION)

        // A tiny interval means this fires on essentially every distinct stream-time
        // advance, i.e. every time block-tracking has something new to react to -
        // NOT wall-clock time (acceptance criterion 4.5).
        context.schedule(Duration.ofMillis(1), PunctuationType.STREAM_TIME) { onPunctuate() }
    }

    override fun process(record: Record<String, RawLogRecord>) {
        val raw = record.value()
        val network = raw.network
        val depth = config.forNetwork(network).confirmationDepth

        val current = blockTrackingStore.get(network)
        val result = BlockTrackingLogic.applyNewBlock(current, network, raw.blockNumber, raw.blockHash, depth)
        blockTrackingStore.put(network, result.state)

        val reorgHeight = result.reorgAtHeight
        if (reorgHeight != null) {
            invalidateAtHeight(network, reorgHeight)
        }
    }

    private fun invalidateAtHeight(network: String, height: Long) {
        val toInvalidate = mutableListOf<Pair<String, ConfirmationState>>()
        confirmationStore.all().use { iter ->
            for (kv in iter) {
                val state = kv.value
                if (state.network == network && state.status == ConfirmationStatus.UNCONFIRMED && state.decodedEvent.blockNumber == height) {
                    toInvalidate += kv.key to state.copy(
                        status = ConfirmationStatus.INVALIDATED,
                        decodedEvent = state.decodedEvent.copy(status = ConfirmationStatus.INVALIDATED),
                    )
                }
            }
        }
        for ((key, invalidated) in toInvalidate) {
            confirmationStore.put(key, invalidated)
            context.forward(Record(key, LifecycleOutput.Confirmation(invalidated), context.currentSystemTimeMs()))
        }
    }

    private fun onPunctuate() {
        val trackedNetworks = mutableListOf<BlockTrackingState>()
        blockTrackingStore.all().use { iter -> for (kv in iter) trackedNetworks += kv.value }

        for (tracking in trackedNetworks) {
            val params = config.forNetwork(tracking.network)
            promoteAndSweepConfirmations(tracking, params.confirmationDepth)
            checkReconciliationAnomalies(tracking, params.reconciliationGapBlocks)
        }
    }

    private fun promoteAndSweepConfirmations(tracking: BlockTrackingState, depth: Int) {
        val toPromote = mutableListOf<Pair<String, ConfirmationState>>()
        val toUpdateSeen = mutableListOf<Pair<String, ConfirmationState>>()
        val toSweep = mutableListOf<String>()

        confirmationStore.all().use { iter ->
            for (kv in iter) {
                val state = kv.value
                if (state.network != tracking.network) continue
                val eventBlock = state.decodedEvent.blockNumber
                when (state.status) {
                    ConfirmationStatus.UNCONFIRMED -> {
                        val seen = ConfirmationLogic.confirmationsSeen(tracking.lastBlock, eventBlock)
                        if (ConfirmationLogic.shouldConfirm(seen, depth)) {
                            toPromote += kv.key to state.copy(
                                status = ConfirmationStatus.CONFIRMED,
                                confirmationsSeen = seen,
                                decodedEvent = state.decodedEvent.copy(status = ConfirmationStatus.CONFIRMED),
                            )
                        } else if (seen != state.confirmationsSeen) {
                            toUpdateSeen += kv.key to state.copy(confirmationsSeen = seen)
                        }
                    }
                    ConfirmationStatus.CONFIRMED, ConfirmationStatus.INVALIDATED -> {
                        if (tracking.lastBlock - eventBlock > depth + terminalRetentionBufferBlocks) {
                            toSweep += kv.key
                        }
                    }
                }
            }
        }

        for ((key, state) in toUpdateSeen) confirmationStore.put(key, state)
        for ((key, state) in toPromote) {
            confirmationStore.put(key, state)
            context.forward(Record(key, LifecycleOutput.Confirmation(state), context.currentSystemTimeMs()))
        }
        for (key in toSweep) confirmationStore.delete(key)
    }

    private fun checkReconciliationAnomalies(tracking: BlockTrackingState, gapBlocks: Int) {
        val toEmit = mutableListOf<Pair<String, ReconciliationAnomaly>>()
        val toRemove = mutableListOf<String>()

        reconciliationStore.all().use { iter ->
            for (kv in iter) {
                val state = kv.value
                if (state.network != tracking.network) continue
                val anomalyType = ReconciliationLogic.checkAnomaly(state, tracking.lastBlock, gapBlocks) ?: continue
                toEmit += kv.key to ReconciliationAnomaly(
                    type = anomalyType,
                    network = state.network,
                    txHash = state.txHash,
                    logIndex = state.logIndex,
                    detectedAtEpochMillis = context.currentSystemTimeMs(),
                    details = "seenWs=${state.seenWs} seenPoll=${state.seenPoll} firstSeenAtEpochMillis=${state.firstSeenAtEpochMillis}",
                )
                toRemove += kv.key
            }
        }

        for (key in toRemove) reconciliationStore.delete(key)
        for ((key, anomaly) in toEmit) context.forward(Record(key, LifecycleOutput.Anomaly(anomaly), context.currentSystemTimeMs()))
    }

    override fun close() {}
}
