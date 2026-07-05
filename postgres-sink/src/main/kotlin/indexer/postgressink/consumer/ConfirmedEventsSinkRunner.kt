package indexer.postgressink.consumer

import indexer.postgressink.db.ConfirmedEventRepository
import indexer.schema.DecodedEventEnvelope
import indexer.schema.json.IndexerJson
import org.apache.kafka.clients.consumer.Consumer
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * Hand-rolled Kafka consumer for confirmed-events-topic (design doc 4.7 -
 * see postgres-sink/README.md for the Kafka Connect JDBC Sink Connector
 * evaluation that led to this choice).
 *
 * Depends on the plain [Consumer] interface (not the concrete
 * KafkaConsumer) so tests can drive it with MockConsumer without a real
 * broker. Offsets are committed synchronously only *after* every record in
 * a poll batch has been upserted - a crash mid-batch re-delivers the whole
 * batch on restart rather than skipping records, which is safe precisely
 * because [ConfirmedEventRepository.upsert] is idempotent.
 */
class ConfirmedEventsSinkRunner(
    private val consumer: Consumer<String, String>,
    private val repository: ConfirmedEventRepository,
    private val pollTimeout: Duration = Duration.ofMillis(500),
) {
    private val log = LoggerFactory.getLogger(ConfirmedEventsSinkRunner::class.java)

    @Volatile
    private var running = true

    /** Signals [run] to stop after its current poll. */
    fun stop() {
        running = false
    }

    /** Blocking poll loop; runs until [stop] is called. */
    fun run() {
        while (running) {
            poll()
        }
    }

    /** Processes exactly one poll's worth of records. Returns the number processed. */
    fun poll(): Int {
        val records = consumer.poll(pollTimeout)
        var processed = 0
        for (record in records) {
            val envelope = IndexerJson.instance.decodeFromString(DecodedEventEnvelope.serializer(), record.value())
            repository.upsert(envelope)
            log.debug(
                "upserted network={} txHash={} logIndex={} status={}",
                envelope.network,
                envelope.txHash,
                envelope.logIndex,
                envelope.status,
            )
            processed++
        }
        if (processed > 0) {
            consumer.commitSync()
        }
        return processed
    }
}
