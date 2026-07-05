// streams-topology is a PURE Kafka Streams topology-building library: no Ktor,
// no HTTP, no embedded server, no application plugin. subscription-api depends
// on it, builds a live KafkaStreams instance from these topology builders, and
// serves Interactive Queries against it. Keeping this module HTTP-free is what
// lets every KTable / join / punctuator be proven end-to-end with
// TopologyTestDriver in milliseconds (design doc section 5.2, test pyramid
// layer 2) with no Ktor/coroutines/Hoplite/Micrometer noise in the classpath.

dependencies {
    implementation(project(":schema"))
    implementation(project(":decoder"))

    implementation(libs.kotlinx.serialization.json)

    implementation(libs.kafka.clients)
    implementation(libs.kafka.streams)

    implementation(libs.slf4j.api)
    testRuntimeOnly(libs.logback.classic)

    testImplementation(libs.kafka.streams.test.utils)
    testImplementation(libs.awaitility.kotlin)
}
