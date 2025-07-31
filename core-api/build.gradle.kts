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