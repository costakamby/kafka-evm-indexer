package indexer.postgressink.testsupport

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import javax.sql.DataSource

/**
 * One real Postgres container (design doc section 5.2 layer 3: "component
 * tests against a real dependency it owns"), started lazily and reused
 * across postgres-sink's component test classes in a single JVM run rather
 * than paying container-startup cost per test class.
 */
object SharedPostgresContainer {
    val container: PostgreSQLContainer<*> by lazy {
        @Suppress("UNCHECKED_CAST")
        (PostgreSQLContainer(DockerImageName.parse("postgres:16")) as PostgreSQLContainer<*>)
            .withDatabaseName("indexer")
            .withUsername("indexer")
            .withPassword("indexer")
            .also { it.start() }
    }

    fun newDataSource(): DataSource {
        val hikariConfig = HikariConfig().apply {
            jdbcUrl = container.jdbcUrl
            username = container.username
            password = container.password
            driverClassName = container.driverClassName
            maximumPoolSize = 4
        }
        return HikariDataSource(hikariConfig)
    }
}
