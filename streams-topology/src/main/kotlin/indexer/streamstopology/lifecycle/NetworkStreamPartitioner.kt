package indexer.streamstopology.lifecycle

import org.apache.kafka.streams.processor.StreamPartitioner

/**
 * Co-partitions a stream by NETWORK rather than by its logical record key.
 *
 * Bug this fixes (found by the integration-tests module's reorg end-to-end test,
 * design doc 4.5/4.8 - only reproducible with the >1-partition topic counts
 * `TopicDefinitions` actually configures for raw-logs-topic/decoded-logs-topic in
 * production; invisible to TopologyTestDriver's typical single-partition test
 * topics): [BlockTrackingProcessor], [ReconciliationProcessor] and the
 * confirmation-store scans in this file all touch state stores added via
 * `builder.addStateStore(...)`, which Kafka Streams instantiates ONE COPY OF PER
 * TASK (i.e. per partition of the co-partitioned sub-topology) - a task can only
 * see and mutate its OWN local slice of BLOCK_TRACKING / RECONCILIATION /
 * CONFIRMATION, never another task's. Two events for the SAME network landing
 * on DIFFERENT tasks means [BlockTrackingProcessor]'s punctuator (which only
 * scans ITS OWN task's local CONFIRMATION store) can permanently miss
 * promoting/invalidating one of them - exactly the failure observed: one event
 * stuck UNCONFIRMED forever, a sibling event on the same network correctly
 * reaching CONFIRMED, purely depending on which task its key happened to hash to.
 *
 * The fix, applied symmetrically in IndexerTopology for BOTH inputs that feed
 * these stores: each does its OWN internal `.selectKey(network).repartition(...)`
 * into a private, network-partitioned internal topic
 * (`raw-logs-by-network`, `decoded-logs-by-network`) immediately before the
 * processor that needs co-location - never by overriding the EXTERNAL topic's
 * own partitioning (raw-logs-topic, decoded-logs-topic keep default key-hash
 * partitioning on their real keys; that's the contract every other consumer of
 * those topics relies on). CONFIRMED_EVENTS/RECONCILIATION_ANOMALIES (pure
 * output sinks, never read back into a co-partitioned store) are unaffected -
 * they stay keyed by EventKey with default partitioning, recovered from the
 * VALUE by the processors that emit them (see [ReconciliationProcessor]'s kdoc).
 *
 * [numPartitions] is whatever the INTERNAL repartition topic is configured
 * with (Kafka Streams sizes it to match the upstream source topic's partition
 * count by default) - this partitioner doesn't need raw-logs-topic and
 * decoded-logs-topic to share a partition count with each other, since each
 * gets its own independently-sized internal repartition topic.
 */
object NetworkStreamPartitioner {
    fun <K, V> forNetwork(networkOf: (V) -> String): StreamPartitioner<K, V> =
        StreamPartitioner { _, _, value, numPartitions -> Math.floorMod(networkOf(value).hashCode(), numPartitions) }
}
