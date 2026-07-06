package indexer.integrationtests.harness

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import indexer.integrationtests.fixtures.AnvilFixture
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import javax.sql.DataSource

/**
 * Per design doc section 5.3, the slow-to-provision dependencies (an Anvil
 * instance forking a real chain over the network, and a Postgres container) are
 * started ONCE per JVM run and reused across every integration test class, with
 * chain state isolated per-test via anvil snapshot/revert and Postgres isolated
 * via TRUNCATE. Kafka, by contrast, is started fresh per test class (see
 * [KafkaTestBroker]) so each class gets clean topics/offsets - far simpler and
 * more robust than trying to scrub a shared broker's topic state between classes.
 *
 * Nothing here is stopped explicitly; Testcontainers' Ryuk reaper tears the
 * containers down on JVM exit (identical pattern to postgres-sink's
 * SharedPostgresContainer).
 */
object SharedInfra {

    val anvil: AnvilFixture by lazy { AnvilFixture.start() }

    private val postgres: PostgreSQLContainer<*> by lazy {
        @Suppress("UNCHECKED_CAST")
        (PostgreSQLContainer(DockerImageName.parse("postgres:16")) as PostgreSQLContainer<*>)
            .withDatabaseName("indexer")
            .withUsername("indexer")
            .withPassword("indexer")
            .also { it.start() }
    }

    /** A pooled DataSource against the shared Postgres, for the postgres-sink instance under test. */
    fun newPostgresDataSource(): DataSource = HikariDataSource(
        HikariConfig().apply {
            jdbcUrl = postgres.jdbcUrl
            username = postgres.username
            password = postgres.password
            driverClassName = postgres.driverClassName
            maximumPoolSize = 4
        },
    )
}
