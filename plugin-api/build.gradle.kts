@file:Suppress("RedundantSuppression")

import java.time.Instant
import java.time.format.DateTimeFormatter

plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
}

repositories {
    maven(url = "https://repo.papermc.io/repository/maven-public/")
}

// Unique module dependencies
dependencies {
    // bring the code part of core-api, but exclude all transitive dependencies
    //   if we did not exclude them, they would be shaded
    //   we want them brought as transitive dependencies in the plugin-api jar too
    api(project(":core-api")) {
        exclude(group = "*", module = "*")
    }

    // Spigot (modified TacoSpigot 1.8, removing some conflicting classes from the build)
    compileOnly("net.techcable.tacospigot:server:1.8.8-R0.2-REDUCED-KC")

    // Kotlin Libraries (transitive dependencies intended for external use)
    api(kotlin("stdlib-jdk8"))
    api(project.property("kotlinx-coroutines-core") as String)
    api(project.property("kotlinx-serialization-core") as String)
    api(project.property("kotlinx-serialization-json-jvm") as String)
    api(project.property("kotlin-reflect") as String)

    // MongoDB
    api(project.property("mongodb-driver-kotlin-coroutine") as String)
    api(project.property("bson-kotlinx") as String)
    api(project.property("slf4j-nop") as String)

    // Guava
    api(project.property("guava") as String) // brings org.jspecify annotations

    // Testing Dependencies
    testImplementation(project.property("kotest-runner-junit5") as String)
    testImplementation(project.property("kotest-assertions-core") as String)
    testImplementation(project.property("kotest-property") as String)
    testImplementation(project.property("kotest-framework-datatest") as String)

    testImplementation(project.property("testcontainers-junit-jupiter") as String)
    testImplementation(project.property("testcontainers-mongodb") as String)
    testImplementation(project.property("testcontainers-core") as String)
    testImplementation(project.property("kotlinx-coroutines-test") as String)

    testRuntimeOnly(project.property("slf4j-nop") as String)

    // MockBukkit
    @Suppress("VulnerableLibrariesLocal")
    testImplementation("org.mockbukkit.mockbukkit:mockbukkit-v1.21:4.76.0")
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