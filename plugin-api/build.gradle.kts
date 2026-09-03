@file:Suppress("RedundantSuppression")

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.jar.JarFile

plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
}

repositories {
    mavenCentral()
    maven(url = "https://repo.papermc.io/repository/maven-public/")
}

// Base MockBukkit Dependency Versions
val mockBukkitModule = "org.mockbukkit.mockbukkit:mockbukkit-v1.21:4.116.3"
val defaultPaperVersion = "1.21.5-R0.1-SNAPSHOT"

// Resolve JUST MockBukkit jar via a detached, resolvable configuration
// This avoids issues with other resolutions that cause resolution cycle failures
fun resolvePaperVersionFromMockBukkit(): String {
    val detached = configurations.detachedConfiguration(
        dependencies.create(mockBukkitModule)
    ).apply {
        // we only need the main jar
        isTransitive = false
    }

    val mockbukkitJar = detached
        .resolve()
        .find { it.name.startsWith("mockbukkit-") }

    return if (mockbukkitJar != null) {
        JarFile(mockbukkitJar).use { jar ->
            jar.manifest?.mainAttributes?.getValue("Paper-Version") ?: defaultPaperVersion
        }
    } else {
        defaultPaperVersion
    }
}
// Evaluate once at configuration time (safe, isolated)
val paperVersion = resolvePaperVersionFromMockBukkit()

// Unique module dependencies
dependencies {
    // bring the code part of core-api, but exclude all transitive dependencies
    //   if we did not exclude them, they would be shaded
    //   we want them brought as transitive dependencies in the plugin-api jar too
    api(project(":core-api")) {
        exclude(group = "*", module = "*")
    }

    // Spigot (modified TacoSpigot 1.8, removing some conflicting classes from the build)
    compileOnly(libs.tacospigot.server)

    // Kotlin Libraries (transitive dependencies intended for external use)
    api(kotlin("stdlib-jdk8"))
    api(libs.kotlinx.coroutines.core)
    api(libs.kotlinx.serialization.core)
    api(libs.kotlinx.serialization.json.jvm)
    api(libs.kotlin.reflect)

    // KamiCommon
    api(libs.kamicommon.standalone.utils)

    // MongoDB
    api(libs.mongodb.driver.kotlin.coroutine)
    api(libs.bson.kotlinx)
    api(libs.slf4j.nop)

    // Guava
    api(libs.guava) // brings org.jspecify annotations

    // Testing Dependencies
    testImplementation(libs.kotest.runner.junit5)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(libs.kotest.property)
    testImplementation(libs.kotest.framework.datatest)

    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.mongodb)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.kotlinx.coroutines.test)

    testRuntimeOnly(libs.slf4j.nop)

    // MockBukkit
    testImplementation(mockBukkitModule)
    @Suppress("VulnerableLibrariesLocal")
    testImplementation("io.papermc.paper:paper-api:${paperVersion}")
}

tasks {
    publish.get().dependsOn(build)

    processResources {
        filteringCharset = Charsets.UTF_8.name()
        val props = mapOf(
            "name" to rootProject.name,
            "version" to project.version,
            "description" to project.description,
            "date" to DateTimeFormatter.ISO_INSTANT.format(Instant.now())
        )
        inputs.properties(props)
        filesMatching("plugin.yml") {
            expand(props)
        }
    }
}

// Configure Publication
publishing {
    // This jar is intended as an API jar, and should ONLY be shaded. It does not function as a Spigot Plugin.
    publications {
        create<MavenPublication>("maven") {
            groupId = rootProject.group.toString() + ".datakache"
            artifactId = "plugin-api"
            version = rootProject.version.toString()
            from(components["java"])
        }
    }

    repositories {
        maven {
            credentials {
                username = System.getenv("LUXIOUS_NEXUS_USER")
                password = System.getenv("LUXIOUS_NEXUS_PASS")
            }
            // Snapshot management
            url = if (rootProject.version.toString().endsWith("-SNAPSHOT")) {
                uri("https://repo.luxiouslabs.net/repository/maven-snapshots/")
            } else {
                uri("https://repo.luxiouslabs.net/repository/maven-releases/")
            }
        }
    }
}