plugins {
    application
}

application {
    mainClass.set("indexer.ingestionws.ApplicationKt")
}

dependencies {
    implementation(project(":schema"))

    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines.core)

    implementation(libs.kafka.clients)

    implementation(libs.ktor.client.core)
    implementation(libs.ktor.client.cio)
    implementation(libs.ktor.client.websockets)

    implementation(libs.hoplite.core)
    implementation(libs.hoplite.yaml)

    implementation(libs.micrometer.core)

    implementation(libs.logback.classic)
    implementation(libs.slf4j.api)

    testImplementation(libs.wiremock)
    testImplementation(libs.awaitility.kotlin)
    testImplementation(libs.kotlinx.coroutines.test)

    // Test-only fake local WS + JSON-RPC HTTP server (component tests, section 5.2
    // layer 3) - never a real chain, never WireMock-for-websockets (WireMock has no
    // real WS support). Kept testImplementation-only, no runtime footprint.
    testImplementation(libs.ktor.server.core)
    testImplementation(libs.ktor.server.cio)
    testImplementation(libs.ktor.server.websockets)
}
