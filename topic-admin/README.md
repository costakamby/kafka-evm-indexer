# topic-admin

Idempotent, checked-in Kafka topic provisioning — no manual `kafka-topics.sh`
commands. [`TopicDefinitions`](src/main/kotlin/indexer/topicadmin/TopicDefinitions.kt)
is the single source of truth for every topic's partition count and cleanup
policy; [`TopicSetup.ensureTopics(admin)`](src/main/kotlin/indexer/topicadmin/TopicSetup.kt)
creates whatever's missing and is a safe no-op otherwise.

## Running

```bash
./gradlew :topic-admin:run --args="localhost:9092"
```

Every service in this project calls `TopicSetup.ensureTopics()` on its own
startup too (Kafka's `auto.create.topics.enable=false` in this project's
`docker-compose.yml`, so topics must exist before any producer/consumer/
Streams instance touches them) — running this standalone is only needed
once, or after adding a new topic to `TopicDefinitions`.

## Adding a new topic

Add an entry to `TopicDefinitions.ALL` with a comment explaining the
partition count and cleanup-policy choice — `subscriptions-topic` and
`poll-progress-topic` are compacted (only the latest value per key matters);
everything else is delete-policy (event streams, not keyed state).
`TopicDefinitionsTest` should get a corresponding assertion.
