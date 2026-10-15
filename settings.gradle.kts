pluginManagement {
    // Include Kotlin & Kotlin Serialization Plugins
    plugins {
        kotlin("jvm") version "2.4.21"
        kotlin("plugin.serialization") version "2.4.21"
    }
}

rootProject.name = "DataKache"
include("core-api")
include("plugin-api")
