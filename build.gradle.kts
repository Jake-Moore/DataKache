@file:Suppress("RedundantSuppression")

import dev.detekt.gradle.Detekt
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    // Java Build Plugins
    id("java")
    id("java-library")
    id("maven-publish")

    // Detekt Code Quality Plugin
    id("dev.detekt") version "2.0.0-alpha.2"

    // Kotlin Plugins
    kotlin("jvm")
    kotlin("plugin.serialization")
}

@Suppress("PropertyName")
val VERSION = "0.4.6"

allprojects {
    group = "com.jakemoore"
    version = VERSION
    description = "A Kotlin-first data library with multi-backend support, in-memory caching, " +
            "and thread-safe updates for Serializable data in multiple environments."

    apply(plugin = "java")
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "dev.detekt")

    // Provision Java 21
    java {
        toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    }

    dependencies {
        detektPlugins("dev.detekt:detekt-rules-ktlint-wrapper:2.0.0-alpha.6")

        // Annotations
        compileOnly("org.jetbrains:annotations:26.1.0")
        testCompileOnly("org.jetbrains:annotations:26.1.0")
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
            events(
                TestLogEvent.PASSED,
                TestLogEvent.SKIPPED,
                TestLogEvent.FAILED,
                // TestLogEvent.STANDARD_ERROR, // Uncomment to debug tests
                // TestLogEvent.STANDARD_OUT, // Uncomment to debug tests
            )
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

        // Use the detekt.yml file from the classpath
        config.setFrom(detektConfig)
    }

    tasks.withType<Detekt>().configureEach {
        autoCorrect = true
        reports {
            html.required.set(false)
            sarif.required.set(false)
            checkstyle.required.set(false)
            markdown.required.set(false)
        }
    }

}

subprojects {
    // Ensure tests run before publishing in each subproject
    tasks.withType<PublishToMavenRepository>().configureEach {
        dependsOn(
            ":core-api:test",
            ":plugin-api:test"
        )
    }
}

// Disable root project build
tasks.jar.get().enabled = false