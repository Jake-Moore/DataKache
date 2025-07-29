plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
}

// Unique module dependencies
dependencies {
    api(project(":core-api"))

    // Spigot (modified TacoSpigot 1.8, removing some conflicting classes from the build)
    compileOnly("net.techcable.tacospigot:server:1.8.8-R0.2-REDUCED-KC")
}

// Configure Publication
publishing {
    // This jar is intended as an API jar, and should ONLY be shaded. It does not function as a Spigot Plugin.
    publications {
        create<MavenPublication>("jarPublication") {
            groupId = rootProject.group.toString()
            artifactId = rootProject.name.lowercase()
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