plugins {
    application
}

application {
    mainClass.set("indexer.ingestionpoll.ApplicationKt")
}

dependencies {
    implementation(project(":schema"))

    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines.core)

    implementation(libs.kafka.clients)

    implementation(libs.ktor.client.core)
    implementation(libs.ktor.client.cio)
    implementation(libs.ktor.client.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)

    implementation(libs.web3j.core)
    implementation(libs.web3j.abi)

    implementation(libs.hoplite.core)
    implementation(libs.hoplite.yaml)

    implementation(libs.micrometer.core)

    implementation(libs.logback.classic)
    implementation(libs.slf4j.api)

    testImplementation(libs.wiremock)
    testImplementation(libs.awaitility.kotlin)
}
