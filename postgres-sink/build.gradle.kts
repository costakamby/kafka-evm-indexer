plugins {
    application
}

application {
    mainClass.set("indexer.postgressink.ApplicationKt")
}

dependencies {
    implementation(project(":schema"))

    implementation(libs.kotlinx.serialization.json)

    implementation(libs.kafka.clients)

    implementation(libs.postgresql.driver)
    implementation(libs.hikaricp)
    implementation(libs.flyway.core)
    implementation(libs.flyway.postgresql)

    implementation(libs.hoplite.core)
    implementation(libs.hoplite.yaml)

    implementation(libs.micrometer.core)

    implementation(libs.logback.classic)
    implementation(libs.slf4j.api)

    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.postgresql)
}
