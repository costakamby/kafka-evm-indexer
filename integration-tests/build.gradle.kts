// Tier-4 full end-to-end integration harness (design doc section 4.8 / 5.2
// layer 4). This is the ONE module that wires every other already-merged module
// together in a single JVM against Testcontainers-managed Kafka + Postgres and a
// Testcontainers-managed Anvil fork, and proves genuine cross-module behaviour
// (reorg -> INVALIDATED, kill-instance -> HA failover) that no lower pyramid
// layer can. It therefore depends on ALL the runtime modules, not just :schema.
//
// The modules below expose their production classes as `implementation` deps
// (not `api`), so their transitive Ktor/Kafka-Streams/etc. libraries are on this
// module's test RUNTIME classpath but NOT its COMPILE classpath - hence every
// library this harness's own code references directly is re-declared explicitly
// as testImplementation here.

dependencies {
    testImplementation(project(":schema"))
    testImplementation(project(":topic-admin"))
    testImplementation(project(":decoder"))
    testImplementation(project(":streams-topology"))
    testImplementation(project(":subscription-api"))
    testImplementation(project(":ingestion-poll"))
    testImplementation(project(":ingestion-ws"))
    testImplementation(project(":postgres-sink"))

    testImplementation(libs.kotlinx.serialization.json)
    testImplementation(libs.kotlinx.coroutines.core)

    testImplementation(libs.kafka.clients)
    testImplementation(libs.kafka.streams)

    // Embedded subscription-api (Ktor server + KafkaStreams in one JVM) wiring.
    testImplementation(libs.ktor.server.core)
    testImplementation(libs.ktor.server.netty)

    // Real ingestion-ws / ingestion-poll HTTP + WS clients pointed at Anvil.
    testImplementation(libs.ktor.client.core)
    testImplementation(libs.ktor.client.cio)
    testImplementation(libs.ktor.client.websockets)

    // Anvil fork control + on-chain fixtures.
    testImplementation(libs.web3j.core)
    testImplementation(libs.web3j.abi)

    // postgres-sink DataSource wiring for the shared Postgres container.
    testImplementation(libs.hikaricp)
    testImplementation(libs.postgresql.driver)

    testImplementation(libs.logback.classic)
    testImplementation(libs.slf4j.api)

    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.awaitility.kotlin)
}

tasks.test {
    // SoakTest (design doc 4.8's lower-priority load/soak bullet - see its kdoc)
    // is a sketch, not a stable, fast, always-green acceptance test - excluded
    // from the normal run (and therefore from CI's merge-gating job) so it never
    // blocks a PR. Run it explicitly with `-PincludeSoakTests`.
    if (!project.hasProperty("includeSoakTests")) {
        useJUnitPlatform { excludeTags("soak") }
    }
}
