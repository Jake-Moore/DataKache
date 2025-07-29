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

val VERSION = "0.0.1-SNAPSHOT"

allprojects {
    group = "com.kamikazejam"
    version = VERSION
    description = "A Kotlin-first data library with multi-backend support, in-memory caching, and thread-safe updates " +
            "for Serializable data in multiple environments."

    apply(plugin = "java")
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "io.gitlab.arturbosch.detekt")

    // Provision Java 21
    java {
        toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    }

    repositories {
        mavenCentral()
        maven("https://repo.luxiouslabs.net/repository/maven-public/")
    }

    dependencies {
        // Kotlin Libraries
        api(kotlin("stdlib-jdk8"))
        api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

        // KotlinX Libraries
        val serializationVer = "1.9.0"
        api("org.jetbrains.kotlinx:kotlinx-serialization-core:${serializationVer}")
        api("org.jetbrains.kotlinx:kotlinx-serialization-json-jvm:${serializationVer}")
        //      Reflect is needed for managing specific KProperty's on Store objects
        api("org.jetbrains.kotlin:kotlin-reflect:2.2.0")

        // MongoDB
        val mongoVer = "5.5.1"
        api("org.mongodb:mongodb-driver-kotlin-coroutine:${mongoVer}")
        api("org.mongodb:bson-kotlinx:${mongoVer}") // BSON for Serialization (for MongoDB)
        api("ch.qos.logback:logback-classic:1.5.18") // Logging for MongoDB

        // Detekt Extensions
        detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.8")

        // Annotations
        compileOnly("org.jetbrains:annotations:26.0.2")
        testCompileOnly("org.jetbrains:annotations:26.0.2")
    }

    // We want UTF-8 for everything
    tasks.withType<JavaCompile> {
        options.encoding = Charsets.UTF_8.name()
    }
    tasks.withType<Javadoc> {
        options.encoding = Charsets.UTF_8.name()
        charset("UTF-8")
    }

    // Register a cleaning task to remove libs outputs and detekt reports
    tasks.register<Delete>("cleanBuild") {
        delete("build/libs")
        delete("build/reports/detekt")
    }
    tasks.build.get().dependsOn("cleanBuild")

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