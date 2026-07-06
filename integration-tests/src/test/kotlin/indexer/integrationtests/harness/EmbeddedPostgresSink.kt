package indexer.integrationtests.harness

import indexer.postgressink.consumer.ConfirmedEventsSinkRunner
import indexer.postgressink.db.ConfirmedEventRepository
import indexer.streamstopology.Topics
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import javax.sql.DataSource

/**
 * A real postgres-sink instance: the production [ConfirmedEventsSinkRunner]
 * consuming confirmed-events-topic and idempotently upserting into the shared
 * Postgres via the production [ConfirmedEventRepository]. Runs its blocking poll
 * loop on a dedicated thread. The repository is exposed so tests can assert on
 * the final materialised row - the true end-to-end deliverable of the pipeline.
 */
class EmbeddedPostgresSink(
    private val bootstrapServers: String,
    dataSource: DataSource,
    private val groupId: String,
) : AutoCloseable {

    private val log = LoggerFactory.getLogger("harness.EmbeddedPostgresSink")

    val repository = ConfirmedEventRepository(dataSource)

    private val consumer = KafkaConsumer<String, String>(
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        },
    )
    private val runner = ConfirmedEventsSinkRunner(consumer, repository, Duration.ofMillis(300))
    private var thread: Thread? = null

    fun start() {
        repository.migrate()
        consumer.subscribe(listOf(Topics.CONFIRMED_EVENTS))
        thread = Thread({
            try {
                runner.run()
            } catch (_: WakeupException) {
                // expected on close()
            } catch (e: Exception) {
                log.error("sink loop crashed", e)
            }
        }, "postgres-sink-runner").also { it.isDaemon = true; it.start() }
    }

    override fun close() {
        runner.stop()
        try {
            consumer.wakeup()
        } catch (_: Exception) {
        }
        thread?.join(5_000)
    }
}
