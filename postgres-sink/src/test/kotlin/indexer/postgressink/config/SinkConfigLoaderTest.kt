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

    @Test
    fun `KAFKA_BOOTSTRAP_SERVERS env var overrides the checked-in default, for bring-your-own-Kafka setups`() {
        val overridden = SinkConfigLoader.load(env = mapOf("KAFKA_BOOTSTRAP_SERVERS" to "my-broker:9092"))

        overridden.kafka.bootstrapServers shouldBe "my-broker:9092"
        overridden.kafka.topic shouldBe "confirmed-events-topic"
    }

    @Test
    fun `POSTGRES_JDBC_URL, POSTGRES_USERNAME and POSTGRES_PASSWORD env vars override the checked-in defaults`() {
        val overridden = SinkConfigLoader.load(
            env = mapOf(
                "POSTGRES_JDBC_URL" to "jdbc:postgresql://my-db-host:5432/mydb",
                "POSTGRES_USERNAME" to "my-user",
                "POSTGRES_PASSWORD" to "my-password",
            ),
        )

        overridden.postgres.jdbcUrl shouldBe "jdbc:postgresql://my-db-host:5432/mydb"
        overridden.postgres.username shouldBe "my-user"
        overridden.postgres.password shouldBe "my-password"
    }

    @Test
    fun `blank override env vars are ignored, not applied as overrides`() {
        val default = SinkConfigLoader.load()

        val overridden = SinkConfigLoader.load(
            env = mapOf(
                "KAFKA_BOOTSTRAP_SERVERS" to " ",
                "POSTGRES_JDBC_URL" to " ",
                "POSTGRES_USERNAME" to " ",
                "POSTGRES_PASSWORD" to " ",
            ),
        )

        overridden shouldBe default
    }
}
