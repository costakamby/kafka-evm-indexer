plugins {
    application
}

application {
    mainClass.set("indexer.ingestionws.ApplicationKt")
}

// `./gradlew :ingestion-ws:run` loads the repo-root .env (WS_RPC_URL_<NETWORK>
// overrides, see AppConfigLoader) into the process environment automatically,
// so running this module doesn't require a manual `export $(cat .env | ...)`
// step first. Only affects the `run` task - running the installed
// distribution's bin script directly still needs .env exported manually.
tasks.named<JavaExec>("run") {
    val envFile = rootProject.file(".env")
    if (envFile.exists()) {
        environment(
            envFile.readLines()
                .filter { it.isNotBlank() && !it.trimStart().startsWith("#") }
                .mapNotNull { line ->
                    val idx = line.indexOf('=')
                    if (idx <= 0) null else line.substring(0, idx).trim() to line.substring(idx + 1).trim()
                }
                .toMap(),
        )
    }
}

dependencies {
    implementation(project(":schema"))

    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines.core)

    implementation(libs.kafka.clients)

    implementation(libs.ktor.client.core)
    implementation(libs.ktor.client.cio)
    implementation(libs.ktor.client.websockets)
    implementation(libs.ktor.client.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)

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
    testImplementation(libs.ktor.server.content.negotiation)
}
