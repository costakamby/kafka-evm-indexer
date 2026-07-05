package indexer.streamstopology.lifecycle

import indexer.schema.DecodedEventEnvelope
import indexer.schema.RawLogRecord
import indexer.schema.ReconciliationAnomaly
import indexer.schema.ktable.BlockTrackingState
import indexer.schema.ktable.ConfirmationState
import indexer.schema.ktable.ReconciliationState
import indexer.streamstopology.config.NetworkTopologyConfig
import indexer.streamstopology.serde.jsonSerdeOf
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state.Stores
import java.time.Instant
import java.util.Properties

/**
 * Test-only assembly of just the lifecycle piece (block-tracking + reconciliation
 * processors, sharing state stores, feeding confirmed-events-topic /
 * reconciliation-anomalies-topic) - deliberately excludes the decode/subscription
 * wiring (already covered by DecodeTopologyTest) so these tests isolate the
 * punctuator/reorg/anomaly logic per the test pyramid's "don't blur layers" rule.
 */
object LifecycleTestTopology {
    const val RAW_LOGS = "raw-logs-topic"
    const val DECODED_LOGS = "decoded-logs-topic"
    const val CONFIRMED_EVENTS = "confirmed-events-topic"
    const val ANOMALIES = "reconciliation-anomalies-topic"

    val stringSerde = Serdes.String()!!
    val rawLogSerde = jsonSerdeOf(RawLogRecord.serializer())
    val decodedSerde = jsonSerdeOf(DecodedEventEnvelope.serializer())
    val anomalySerde = jsonSerdeOf(ReconciliationAnomaly.serializer())
    val blockTrackingSerde = jsonSerdeOf(BlockTrackingState.serializer())
    val reconciliationSerde = jsonSerdeOf(ReconciliationState.serializer())
    val confirmationSerde = jsonSerdeOf(ConfirmationState.serializer())

    fun build(config: NetworkTopologyConfig): Topology {
        val builder = StreamsBuilder()

        builder.addStateStore(
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(StoreNames.BLOCK_TRACKING), stringSerde, blockTrackingSerde),
        )
        builder.addStateStore(
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(StoreNames.RECONCILIATION), stringSerde, reconciliationSerde),
        )
        builder.addStateStore(
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(StoreNames.CONFIRMATION), stringSerde, confirmationSerde),
        )

        val fromBlockTracking = builder
            .stream(RAW_LOGS, Consumed.with(stringSerde, rawLogSerde))
            .selectKey({ _, raw -> raw.network }, Named.`as`("rekey-raw-logs-by-network"))
            .process(
                ProcessorSupplier<String, RawLogRecord, String, LifecycleOutput> { BlockTrackingProcessor(config) },
                Named.`as`("block-tracking-processor"),
                StoreNames.BLOCK_TRACKING, StoreNames.RECONCILIATION, StoreNames.CONFIRMATION,
            )

        val fromReconciliation = builder
            .stream(DECODED_LOGS, Consumed.with(stringSerde, decodedSerde))
            .process(
                ProcessorSupplier<String, DecodedEventEnvelope, String, LifecycleOutput> { ReconciliationProcessor() },
                Named.`as`("reconciliation-processor"),
                StoreNames.RECONCILIATION, StoreNames.CONFIRMATION,
            )

        fromBlockTracking.merge(fromReconciliation)
            .split(Named.`as`("lifecycle-output-"))
            .branch(
                { _, out -> out is LifecycleOutput.Confirmation },
                Branched.withConsumer { s ->
                    s.mapValues { (it as LifecycleOutput.Confirmation).state.decodedEvent }
                        .to(CONFIRMED_EVENTS, Produced.with(stringSerde, decodedSerde))
                },
            )
            .defaultBranch(
                Branched.withConsumer { s ->
                    s.mapValues { (it as LifecycleOutput.Anomaly).anomaly }
                        .to(ANOMALIES, Produced.with(stringSerde, anomalySerde))
                },
            )

        return builder.build()
    }

    fun newDriver(config: NetworkTopologyConfig, startTime: Instant = Instant.ofEpochMilli(0)): TopologyTestDriver {
        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "lifecycle-test")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
        }
        return TopologyTestDriver(build(config), props, startTime)
    }
}
