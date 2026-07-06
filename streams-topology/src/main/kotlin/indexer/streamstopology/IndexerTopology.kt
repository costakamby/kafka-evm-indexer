package indexer.streamstopology

import indexer.decoder.EventDecoder
import indexer.schema.DecodedEventEnvelope
import indexer.schema.RawLogRecord
import indexer.schema.ReconciliationAnomaly
import indexer.schema.SubscriptionRecord
import indexer.schema.ktable.BlockTrackingState
import indexer.schema.ktable.ConfirmationState
import indexer.schema.ktable.ReconciliationState
import indexer.streamstopology.config.NetworkTopologyConfig
import indexer.streamstopology.decode.DecodeTopology
import indexer.streamstopology.lifecycle.BlockTrackingProcessor
import indexer.streamstopology.lifecycle.LifecycleOutput
import indexer.streamstopology.lifecycle.NetworkStreamPartitioner
import indexer.streamstopology.lifecycle.ReconciliationProcessor
import indexer.streamstopology.lifecycle.StoreNames
import indexer.streamstopology.serde.jsonSerdeOf
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.Repartitioned
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores

/**
 * Assembles the ONE Kafka Streams topology this project runs (architecture
 * note in the build brief): decode (raw-logs-topic + subscriptions-topic ->
 * decoded-logs-topic / decode-dead-letter-topic), then reconciliation +
 * block-tracking + confirmation (-> confirmed-events-topic /
 * reconciliation-anomalies-topic). subscription-api builds a live KafkaStreams
 * instance from this at startup, in the SAME process as its Ktor server, and
 * uses that instance's local state stores (including the GlobalKTable it adds
 * separately for subscriptions Interactive Queries) to serve REST reads.
 *
 * Pure topology-building code: no Kafka Streams instance is created or
 * started here.
 */
object IndexerTopology {

    fun build(config: NetworkTopologyConfig, decoder: EventDecoder): Topology {
        val builder = StreamsBuilder()
        val stringSerde = Serdes.String()
        val rawLogSerde = jsonSerdeOf(RawLogRecord.serializer())
        val decodedSerde = jsonSerdeOf(DecodedEventEnvelope.serializer())
        val anomalySerde = jsonSerdeOf(ReconciliationAnomaly.serializer())

        // GlobalKTable: every instance holds the FULL subscriptions table locally,
        // so subscription-api's REST layer can answer any Interactive Query
        // against its own local store, no cross-instance forwarding needed
        // (architecture note in the build brief; acceptance criterion 4.1).
        // subscriptions-topic can only be registered as ONE kind of source in a
        // topology, so the decode step below reads this SAME store rather than
        // re-sourcing the topic as an independent stream - see DecodeTopology's
        // kdoc for why.
        builder.globalTable(
            Topics.SUBSCRIPTIONS,
            Consumed.with(stringSerde, jsonSerdeOf(SubscriptionRecord.serializer())),
            // Materialized.as(String) would default to a TIMESTAMPED store
            // (ValueAndTimestamp<V> wrapper, KIP-258) - passing an explicit
            // StoreSupplier forces a plain KeyValueStore<String, SubscriptionRecord>
            // so Processor API code reading it via context.getStateStore(...)
            // gets the value type directly, not a wrapper.
            Materialized.`as`<String, SubscriptionRecord>(
                Stores.persistentKeyValueStore(Topics.SUBSCRIPTIONS_STORE),
            )
                .withKeySerde(stringSerde)
                .withValueSerde(jsonSerdeOf(SubscriptionRecord.serializer())),
        )

        DecodeTopology.addTo(
            builder,
            subscriptionsStoreName = Topics.SUBSCRIPTIONS_STORE,
            rawLogsTopic = Topics.RAW_LOGS,
            decodedLogsTopic = Topics.DECODED_LOGS,
            deadLetterTopic = Topics.DECODE_DEAD_LETTER,
            decoder = decoder,
        )

        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(StoreNames.BLOCK_TRACKING),
                stringSerde,
                jsonSerdeOf(BlockTrackingState.serializer()),
            ),
        )
        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(StoreNames.RECONCILIATION),
                stringSerde,
                jsonSerdeOf(ReconciliationState.serializer()),
            ),
        )
        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(StoreNames.CONFIRMATION),
                stringSerde,
                jsonSerdeOf(ConfirmationState.serializer()),
            ),
        )

        // BLOCK_TRACKING/RECONCILIATION/CONFIRMATION are addStateStore(...) stores -
        // Kafka Streams gives each TASK (i.e. each partition of this co-partitioned
        // sub-topology) its OWN local copy; a task can never see another task's slice.
        // With >1 partition (production's real config - see TopicDefinitions), decoded
        // events for one network MUST land on the exact same task as that network's
        // raw-log-driven block-tracking updates, or BlockTrackingProcessor's punctuator
        // can permanently miss promoting/invalidating them (see NetworkStreamPartitioner's
        // kdoc for the full bug this fixes - found by this project's reorg end-to-end
        // test). An explicit `.repartition(...)` with a NAMED stream partitioner (rather
        // than the implicit auto-repartition selectKey would otherwise trigger, which
        // uses Kafka's default key-hash partitioner) is what makes this deterministic and
        // exactly matches decoded-logs-topic's own output partitioner in DecodeTopology.
        val fromBlockTracking = builder
            .stream(Topics.RAW_LOGS, Consumed.with(stringSerde, rawLogSerde))
            .selectKey({ _, raw -> raw.network }, Named.`as`("rekey-raw-logs-by-network-for-tracking"))
            .repartition(
                Repartitioned.with(stringSerde, rawLogSerde)
                    .withName("raw-logs-by-network")
                    .withStreamPartitioner(NetworkStreamPartitioner.forNetwork<String, RawLogRecord> { it.network }),
            )
            .process(
                ProcessorSupplier<String, RawLogRecord, String, LifecycleOutput> { BlockTrackingProcessor(config) },
                Named.`as`("block-tracking-processor"),
                StoreNames.BLOCK_TRACKING, StoreNames.RECONCILIATION, StoreNames.CONFIRMATION,
            )

        val fromReconciliation = builder
            .stream(Topics.DECODED_LOGS, Consumed.with(stringSerde, decodedSerde))
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
                        .to(Topics.CONFIRMED_EVENTS, Produced.with(stringSerde, decodedSerde))
                },
            )
            .defaultBranch(
                Branched.withConsumer { s ->
                    s.mapValues { (it as LifecycleOutput.Anomaly).anomaly }
                        .to(Topics.RECONCILIATION_ANOMALIES, Produced.with(stringSerde, anomalySerde))
                },
            )

        return builder.build()
    }
}
