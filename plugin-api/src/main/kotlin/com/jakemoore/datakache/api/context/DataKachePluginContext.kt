package com.jakemoore.datakache.api.context

import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.DataKacheContext
import com.jakemoore.datakache.api.config.DataKachePluginConfig
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.logging.PluginLoggerService
import org.bukkit.plugin.java.JavaPlugin
import java.io.File

class DataKachePluginContext(private val plugin: JavaPlugin) : DataKacheContext {
    override val logger: LoggerService = PluginLoggerService(plugin)
    override val config: DataKacheConfig = DataKachePluginConfig.loadFromPluginResources(plugin)
    override val logFolder: File
        get() = plugin.dataFolder
}
