package com.jakemoore.datakache.api.context

import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.DataKacheContext
import com.jakemoore.datakache.api.config.DataKachePluginConfig
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.logging.PluginLoggerService
import org.bukkit.configuration.file.FileConfiguration
import org.bukkit.plugin.java.JavaPlugin
import java.io.File

class DataKachePluginContext(
    private val plugin: JavaPlugin,
    val fileConfiguration: FileConfiguration = DataKachePluginConfig.loadFileConfiguration(
        plugin = plugin,
        resourceFile = "DataKache.yml",
    )
) : DataKacheContext {
    override val logger: LoggerService = PluginLoggerService(plugin)
    override val config: DataKacheConfig = DataKachePluginConfig.loadFromFileConfiguration(fileConfiguration)
    override val logFolder: File
        get() = plugin.dataFolder
}
