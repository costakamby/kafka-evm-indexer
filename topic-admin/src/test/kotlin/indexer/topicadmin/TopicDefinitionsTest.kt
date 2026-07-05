package indexer.topicadmin

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

/**
 * Design doc Phase 0 step 5: subscriptions-topic must be compacted;
 * raw-logs/decoded-logs (and the rest of the pipeline's topics) must not be.
 * Pure logic, no broker needed - layer 1 of the test pyramid (section 5.2).
 */
class TopicDefinitionsTest {

    @Test
    fun `subscriptions-topic is compacted`() {
        val definition = TopicDefinitions.ALL.first { it.name == "subscriptions-topic" }

        definition.cleanupPolicy shouldBe CleanupPolicy.COMPACT
    }

    @Test
    fun `raw-logs-topic and decoded-logs-topic use deletion cleanup policy`() {
        val rawLogs = TopicDefinitions.ALL.first { it.name == "raw-logs-topic" }
        val decodedLogs = TopicDefinitions.ALL.first { it.name == "decoded-logs-topic" }

        rawLogs.cleanupPolicy shouldBe CleanupPolicy.DELETE
        decodedLogs.cleanupPolicy shouldBe CleanupPolicy.DELETE
    }

    @Test
    fun `every topic has a distinct name and at least one partition`() {
        val names = TopicDefinitions.ALL.map { it.name }

        names.toSet().size shouldBe names.size
        TopicDefinitions.ALL.forEach { definition ->
            assert(definition.partitions >= 1) { "${definition.name} must have at least 1 partition" }
        }
    }
}
