pluginManagement {
    // Include Kotlin & Kotlin Serialization Plugins
    plugins {
        kotlin("jvm") version "2.2.20"
        kotlin("plugin.serialization") version "2.2.20"
    }
}

rootProject.name = "DataKache"
include("core-api")
include("plugin-api")