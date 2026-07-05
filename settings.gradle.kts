rootProject.name = "kafka-evm-indexer"

include(
    "schema",
    "topic-admin",
    "subscription-api",
    "ingestion-ws",
    "ingestion-poll",
    "decoder",
    "streams-topology",
    "postgres-sink",
    "integration-tests",
)
