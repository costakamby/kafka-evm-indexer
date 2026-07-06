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

    // `./gradlew :<module>:run` loads the repo-root .env (RPC_URL_<NETWORK> /
    // WS_RPC_URL_<NETWORK> overrides, see ingestion-ws/ingestion-poll's
    // AppConfigLoader) into the process environment automatically, so running
    // a module doesn't require a manual `export $(cat .env | ...)` step
    // first. Only affects the `run` task - running an installed
    // distribution's bin script directly still needs .env exported manually.
    pluginManager.withPlugin("application") {
        tasks.withType<JavaExec>().matching { it.name == "run" }.configureEach {
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
    }
}
