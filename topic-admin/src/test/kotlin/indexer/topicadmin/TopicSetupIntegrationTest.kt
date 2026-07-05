package indexer.topicadmin

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.common.config.ConfigResource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.ConfluentKafkaContainer
import java.util.Properties

/**
 * Component test (test pyramid layer 3, section 5.2): proves TopicSetup's
 * actual AdminClient IO against a real broker - that ensureTopics() is
 * idempotent, and that the created topics carry the exact partition count
 * and cleanup.policy config from TopicDefinitions.
 */
@Testcontainers
class TopicSetupIntegrationTest {

    companion object {
        private val kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.7.1")
        private lateinit var admin: Admin

        @JvmStatic
        @BeforeAll
        fun startKafka() {
            kafka.start()
            val props = Properties()
            props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
            admin = Admin.create(props)
        }

        @JvmStatic
        @AfterAll
        fun stopKafka() {
            admin.close()
            kafka.stop()
        }
    }

    @Test
    fun `ensureTopics creates every topic with the configured partitions and cleanup policy`() {
        TopicSetup.ensureTopics(admin)

        val topicNames = TopicDefinitions.ALL.map { it.name }.toSet()
        val descriptions = admin.describeTopics(topicNames).allTopicNames().get()

        TopicDefinitions.ALL.forEach { definition ->
            val description = descriptions.getValue(definition.name)
            assertEquals(definition.partitions, description.partitions().size, "partition count for ${definition.name}")

            val resource = ConfigResource(ConfigResource.Type.TOPIC, definition.name)
            val config = admin.describeConfigs(listOf(resource)).all().get().getValue(resource)
            val cleanupPolicy: ConfigEntry = config.get("cleanup.policy")
            val expected = if (definition.cleanupPolicy == CleanupPolicy.COMPACT) "compact" else "delete"
            assertEquals(expected, cleanupPolicy.value(), "cleanup.policy for ${definition.name}")
        }
    }

    @Test
    fun `ensureTopics is idempotent - calling it twice does not fail`() {
        TopicSetup.ensureTopics(admin)
        TopicSetup.ensureTopics(admin)

        val topicNames = TopicDefinitions.ALL.map { it.name }.toSet()
        val allTopics = admin.listTopics().names().get()
        assertTrue(allTopics.containsAll(topicNames))
    }
}
