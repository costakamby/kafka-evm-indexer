package indexer.topicadmin

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import org.slf4j.LoggerFactory
import java.util.concurrent.ExecutionException

object TopicSetup {
    private val log = LoggerFactory.getLogger(TopicSetup::class.java)

    /** Idempotent: creates any topic from [definitions] that doesn't already exist. */
    fun ensureTopics(admin: Admin, definitions: List<TopicDefinition> = TopicDefinitions.ALL) {
        val existing = admin.listTopics().names().get()
        val missing = definitions.filter { it.name !in existing }
        if (missing.isEmpty()) {
            log.info("all {} topics already exist", definitions.size)
            return
        }

        val newTopics = missing.map { it.toNewTopic() }
        try {
            admin.createTopics(newTopics).all().get()
            log.info("created {} topics: {}", newTopics.size, missing.map { it.name })
        } catch (e: ExecutionException) {
            if (e.cause is TopicExistsException) {
                log.info("topic(s) already existed, created concurrently by another instance")
            } else {
                throw e
            }
        }
    }

    private fun TopicDefinition.toNewTopic(): NewTopic =
        NewTopic(name, partitions, replicationFactor).configs(
            mapOf("cleanup.policy" to if (cleanupPolicy == CleanupPolicy.COMPACT) "compact" else "delete"),
        )
}
