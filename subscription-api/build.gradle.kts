plugins {
    application
}

application {
    mainClass.set("indexer.subscriptionapi.ApplicationKt")
}

dependencies {
    implementation(project(":schema"))
    implementation(project(":decoder"))
    implementation(project(":streams-topology"))
    implementation(project(":topic-admin"))

    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines.core)

    implementation(libs.kafka.clients)
    implementation(libs.kafka.streams)

    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)
    implementation(libs.ktor.server.metrics.micrometer)
    implementation(libs.ktor.server.call.logging)

    implementation(libs.hoplite.core)
    implementation(libs.hoplite.yaml)

    implementation(libs.micrometer.core)
    implementation(libs.micrometer.registry.prometheus)

    implementation(libs.logback.classic)
    implementation(libs.slf4j.api)

    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.kafka.streams.test.utils)
    testImplementation(libs.awaitility.kotlin)
}
