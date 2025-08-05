package com.jakemoore.datakache.api.config

import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.mode.StorageMode
import org.bukkit.configuration.file.FileConfiguration
import org.bukkit.configuration.file.YamlConfiguration
import org.bukkit.plugin.java.JavaPlugin
import java.io.File

object DataKachePluginConfig {
    fun loadFromFileConfiguration(
        config: FileConfiguration
    ): DataKacheConfig {
        return DataKacheConfig(
            databaseNamespace = config.getString("database-namespace", "global"),
            debug = config.getBoolean("debug", true),
            storageMode = StorageMode.valueOf(config.getString("storage.mode")),

            // Load MongoDB Connection Details
            mongoURI = config.getString("storage.MONGODB.uri"),
        )
    }

    fun loadFileConfiguration(
        plugin: JavaPlugin,
        resourceFile: String,
    ): FileConfiguration {
        val file = File(plugin.dataFolder, resourceFile)
        if (!file.exists()) {
            file.parentFile.mkdirs()
            plugin.saveResource(resourceFile, false)
        }
        return YamlConfiguration().also {
            it.load(file)
        }
    }
}
