package indexer.integrationtests.harness

import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * A single-node KRaft Kafka broker (the `apache/kafka` native image, matching
 * this project's Kafka client version) started per integration test class so
 * every class sees clean topics and offsets. Wraps Testcontainers'
 * [KafkaContainer] purely to give the harness a small, named lifecycle handle.
 */
class KafkaTestBroker : AutoCloseable {
    private val container = KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"))

    fun start(): KafkaTestBroker {
        container.start()
        return this
    }

    val bootstrapServers: String get() = container.bootstrapServers

    override fun close() {
        container.stop()
    }
}
