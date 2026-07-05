plugins {
    application
}

application {
    mainClass.set("indexer.topicadmin.MainKt")
}

dependencies {
    implementation(libs.kafka.clients)
    implementation(libs.logback.classic)
    implementation(libs.slf4j.api)

    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.awaitility.kotlin)
}
