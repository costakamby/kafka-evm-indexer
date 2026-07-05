dependencies {
    implementation(project(":schema"))

    implementation(libs.kotlinx.serialization.json)

    implementation(libs.web3j.core)
    implementation(libs.web3j.abi)

    implementation(libs.logback.classic)
    implementation(libs.slf4j.api)
}
