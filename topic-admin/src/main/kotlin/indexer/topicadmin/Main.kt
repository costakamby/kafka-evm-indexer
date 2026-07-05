package indexer.topicadmin

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import java.util.Properties

/** Checked-in, idempotent topic provisioning - run via `gradle :topic-admin:run`. */
fun main(args: Array<String>) {
    val bootstrapServers = args.firstOrNull() ?: System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
    val props = Properties().apply {
        put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    }
    Admin.create(props).use { admin ->
        TopicSetup.ensureTopics(admin)
    }
}
