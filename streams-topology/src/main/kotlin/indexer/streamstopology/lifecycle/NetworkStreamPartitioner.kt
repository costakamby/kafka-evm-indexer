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
 * CONFIRMATION, never another task's.
 *
 * Raw-logs-topic is deliberately rekeyed by network before block-tracking
 * (`rekey-raw-logs-by-network-for-tracking`), so ALL of one network's block
 * height/hash observations funnel to exactly ONE task. But decoded-logs-topic
 * (feeding [ReconciliationProcessor], which seeds/updates CONFIRMATION store
 * entries) was left keyed by [indexer.schema.EventKey] (network:txHash:logIndex) -
 * a DIFFERENT partitioning scheme. With >1 partition, two events on the SAME
 * network land on DIFFERENT tasks whenever their EventKey hashes differently,
 * so [BlockTrackingProcessor]'s punctuator (which only scans ITS OWN task's local
 * CONFIRMATION store) can permanently miss promoting/invalidating them - exactly
 * the failure observed: one event stuck UNCONFIRMED forever, a sibling event on
 * the same network correctly reaching CONFIRMED, purely depending on which task
 * its txHash happened to hash to.
 *
 * The fix: make decoded-logs-topic's OUTPUT PARTITION (not its logical record
 * key - EventKey is preserved unchanged for point lookups) depend on the
 * decoded event's network, using the exact same hash formula as the raw-logs
 * rekey below. This guarantees every event for a given network lands on the
 * SAME task that owns that network's block-tracking state, restoring correct
 * cross-store visibility, while leaving CONFIRMED_EVENTS/RECONCILIATION_ANOMALIES
 * (pure output sinks, never read back into a co-partitioned store) unaffected.
 *
 * Assumes raw-logs-topic and decoded-logs-topic have the SAME partition count
 * (true today per [indexer.topicadmin.TopicDefinitions] - both 6) - co-partitioning
 * by the same formula only lines up if [numPartitions] matches on both sides.
 */
object NetworkStreamPartitioner {
    fun <K, V> forNetwork(networkOf: (V) -> String): StreamPartitioner<K, V> =
        StreamPartitioner { _, _, value, numPartitions -> Math.floorMod(networkOf(value).hashCode(), numPartitions) }
}
