pluginManagement {
    // Include Kotlin & Kotlin Serialization Plugins
    plugins {
        kotlin("jvm") version "2.3.10"
        kotlin("plugin.serialization") version "2.3.10"
    }
}

rootProject.name = "DataKache"
include("core-api")
include("plugin-api")
