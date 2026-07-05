package indexer.postgressink

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import indexer.postgressink.config.SinkConfigLoader
import indexer.postgressink.consumer.ConfirmedEventsSinkRunner
import indexer.postgressink.db.ConfirmedEventRepository
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties

private val log = LoggerFactory.getLogger("indexer.postgressink.Application")

/**
 * postgres-sink entrypoint: a hand-rolled consumer (design doc 4.7 - see
 * README.md for the Kafka Connect JDBC Sink Connector evaluation) that
 * materializes confirmed-events-topic into Postgres, idempotently upserting
 * on (network, tx_hash, log_index). Run via `gradle :postgres-sink:run`.
 */
fun main() {
    val config = SinkConfigLoader.load()

    val dataSource = HikariDataSource(
        HikariConfig().apply {
            jdbcUrl = config.postgres.jdbcUrl
            username = config.postgres.username
            password = config.postgres.password
        },
    )

    val repository = ConfirmedEventRepository(dataSource)
    repository.migrate()

    val consumerProps = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
        put(ConsumerConfig.GROUP_ID_CONFIG, config.kafka.groupId)
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        // offsets are committed by ConfirmedEventsSinkRunner only after a
        // successful upsert - never auto-commit ahead of the actual write.
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    }
    val consumer = KafkaConsumer<String, String>(consumerProps)
    consumer.subscribe(listOf(config.kafka.topic))

    val runner = ConfirmedEventsSinkRunner(consumer, repository, Duration.ofMillis(config.kafka.pollTimeoutMs))

    Runtime.getRuntime().addShutdownHook(
        Thread {
            log.info("shutting down postgres-sink")
            runner.stop()
            consumer.wakeup()
        },
    )

    log.info(
        "postgres-sink starting: topic={} groupId={} bootstrapServers={}",
        config.kafka.topic,
        config.kafka.groupId,
        config.kafka.bootstrapServers,
    )
    runner.run()
}
