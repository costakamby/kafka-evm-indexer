package indexer.postgressink.config

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * application.yaml is loaded via Hoplite into a typed SinkConfig, matching
 * the docker-compose dev stack's Kafka (localhost:9092) and Postgres
 * (localhost:15432, db/user/password "indexer") defaults.
 */
class SinkConfigLoaderTest {

    @Test
    fun `loads kafka config pointing at confirmed-events-topic`() {
        val config = SinkConfigLoader.load()

        config.kafka.bootstrapServers shouldBe "localhost:9092"
        config.kafka.topic shouldBe "confirmed-events-topic"
        config.kafka.groupId shouldBe "postgres-sink"
    }

    @Test
    fun `loads postgres config matching the docker-compose dev stack`() {
        val config = SinkConfigLoader.load()

        config.postgres.jdbcUrl shouldBe "jdbc:postgresql://localhost:15432/indexer"
        config.postgres.username shouldBe "indexer"
        config.postgres.password shouldBe "indexer"
    }
}
