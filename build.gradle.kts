plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
}

val junitJupiter = libs.junit.jupiter
val junitPlatformLauncher = libs.junit.platform.launcher
val kotestAssertionsCore = libs.kotest.assertions.core
val mockkLib = libs.mockk

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.serialization")

    repositories {
        mavenCentral()
    }

    extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
        jvmToolchain(21)
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }

    dependencies {
        add("testImplementation", junitJupiter)
        add("testRuntimeOnly", junitPlatformLauncher)
        add("testImplementation", kotestAssertionsCore)
        add("testImplementation", mockkLib)
    }
}
