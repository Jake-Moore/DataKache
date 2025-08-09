import org.gradle.api.tasks.testing.logging.TestLogEvent


plugins {
    // Java Build Plugins
    id("java")
    id("java-library")
    id("maven-publish")

    // Detekt Code Quality Plugin
    id("io.gitlab.arturbosch.detekt") version "1.23.8"

    // Kotlin Plugins
    kotlin("jvm")
    kotlin("plugin.serialization")
}

@Suppress("PropertyName")
val VERSION = "0.3.2"

ext {
    // KotlinX
    val coroutinesVer = "1.10.2"
    set("kotlinx-coroutines-core", "org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVer")
    set("kotlinx-coroutines-test", "org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutinesVer")
    val serializationVer = "1.9.0"
    set("kotlinx-serialization-core", "org.jetbrains.kotlinx:kotlinx-serialization-core:${serializationVer}")
    set("kotlinx-serialization-json-jvm", "org.jetbrains.kotlinx:kotlinx-serialization-json-jvm:${serializationVer}")
    // Reflect is needed for managing specific KProperty's on Store objects
    set("kotlin-reflect", "org.jetbrains.kotlin:kotlin-reflect:2.2.0")

    // MongoDB Driver + Kotlin Support
    val mongoVer = "5.5.1"
    set("mongodb-driver-kotlin-coroutine", "org.mongodb:mongodb-driver-kotlin-coroutine:${mongoVer}")
    set("bson-kotlinx", "org.mongodb:bson-kotlinx:${mongoVer}") // BSON for Serialization (for MongoDB)
    set("logback-classic", "ch.qos.logback:logback-classic:1.5.18") // Logging for MongoDB

    // Google Guava (for CacheBuilder)
    set("guava", "com.google.guava:guava:33.4.8-jre")

    // Testing Dependencies
    val kotestVer = "5.9.1"
    set("kotest-runner-junit5", "io.kotest:kotest-runner-junit5:${kotestVer}")
    set("kotest-assertions-core", "io.kotest:kotest-assertions-core:${kotestVer}")
    set("kotest-property", "io.kotest:kotest-property:${kotestVer}")
    set("kotest-framework-datatest", "io.kotest:kotest-framework-datatest:${kotestVer}")

    val testcontainersVer = "1.21.3"
    set("testcontainers-junit-jupiter", "org.testcontainers:junit-jupiter:${testcontainersVer}")
    // NOTE: the MongoDB container automatically sets up its own single-node replica set
    //       This means it supports retryable writes and transactions automatically.
    set("testcontainers-mongodb", "org.testcontainers:mongodb:${testcontainersVer}")
    set("testcontainers-core", "org.testcontainers:testcontainers:${testcontainersVer}")
}

allprojects {
    group = "com.jakemoore"
    version = VERSION
    description = "A Kotlin-first data library with multi-backend support, in-memory caching, " +
            "and thread-safe updates for Serializable data in multiple environments."

    apply(plugin = "java")
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "io.gitlab.arturbosch.detekt")

    // Provision Java 21
    java {
        toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    }

    dependencies {
        detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.8")

        // Annotations
        compileOnly("org.jetbrains:annotations:26.0.2")
        testCompileOnly("org.jetbrains:annotations:26.0.2")
    }

    repositories {
        mavenCentral()
        maven("https://repo.luxiouslabs.net/repository/maven-public/")
    }

    // We want UTF-8 for everything
    tasks.withType<JavaCompile> {
        options.encoding = Charsets.UTF_8.name()
    }
    tasks.withType<Javadoc> {
        options.encoding = Charsets.UTF_8.name()
        charset("UTF-8")
    }

    // Configure Kotest to run with JUnit Platform
    tasks.withType<Test>().configureEach {
        useJUnitPlatform()

        testLogging {
            showExceptions = true
            showStackTraces = true

            // log all event types
            events(TestLogEvent.PASSED, TestLogEvent.SKIPPED, TestLogEvent.FAILED, TestLogEvent.STANDARD_ERROR)
        }
    }

    // Register a cleaning task to remove libs outputs and detekt reports
    tasks.register<Delete>("cleanBuild") {
        delete("build/libs")
        delete("build/reports/detekt")
    }
    tasks.jar.get().dependsOn("cleanBuild")

    // Configure detekt
    val detektConfig = rootProject.layout.projectDirectory.file(".detekt/detekt.yml").asFile
    detekt {
        allRules = true
        autoCorrect = true
        buildUponDefaultConfig = true
        parallel = true

        reports {
            html.required.set(false)
            xml.required.set(false)
            txt.required.set(false)
            sarif.required.set(false)
        }

        // Use the detekt.yml file from the classpath
        config.setFrom(detektConfig)
    }
}

// Disable root project build
tasks.jar.get().enabled = false