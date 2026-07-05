dependencies {
    implementation(project(":schema"))

    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines.core)

    implementation(libs.kafka.clients)

    implementation(libs.web3j.core)
    implementation(libs.web3j.abi)

    implementation(libs.logback.classic)
    implementation(libs.slf4j.api)

    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.awaitility.kotlin)
    testImplementation(libs.postgresql.driver)
}
