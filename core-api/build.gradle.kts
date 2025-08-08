import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
}

dependencies {
    // Kotlin Libraries
    api(kotlin("stdlib-jdk8"))
    api(project.property("kotlinx-coroutines-core") as String)
    api(project.property("kotlinx-serialization-core") as String)
    api(project.property("kotlinx-serialization-json-jvm") as String)
    api(project.property("kotlin-reflect") as String)

    // MongoDB
    api(project.property("mongodb-driver-kotlin-coroutine") as String)
    api(project.property("bson-kotlinx") as String)
    api(project.property("logback-classic") as String)

    api(project.property("guava") as String)

    // Testing Dependencies
    testImplementation(project.property("kotest-runner-junit5") as String)
    testImplementation(project.property("kotest-assertions-core") as String)
    testImplementation(project.property("kotest-property") as String)
    testImplementation(project.property("kotest-framework-datatest") as String)

    testImplementation(project.property("testcontainers-junit-jupiter") as String)
    testImplementation(project.property("testcontainers-mongodb") as String)
    testImplementation(project.property("testcontainers-core") as String)

    testImplementation(project.property("logback-test") as String)
    testImplementation(project.property("kotlinx-coroutines-core") as String)
}

tasks {
    publish.get().dependsOn(build)
}

// Configure Kotest to run with JUnit Platform
tasks.withType<Test>().configureEach {
    useJUnitPlatform()

    testLogging {
        showExceptions = true
        showStackTraces = true

        // log all event types
        events("passed", "skipped", "failed", TestLogEvent.STANDARD_ERROR)
    }
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