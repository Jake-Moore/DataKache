plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
}

dependencies {
    // Kotlin Libraries
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

    api(libs.guava)

    // Testing Dependencies
    testImplementation(libs.kotest.runner.junit5)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(libs.kotest.property)

    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.mongodb)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.kotlinx.coroutines.test)

    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.slf4j.nop)
}

tasks {
    publish.get().dependsOn(build)
}

// Configure Publication
publishing {
    // This jar is intended as an API jar, and should ONLY be shaded.
    publications {
        create<MavenPublication>("jarPublication") {
            groupId = rootProject.group.toString() + ".datakache"
            artifactId = "core-api"
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