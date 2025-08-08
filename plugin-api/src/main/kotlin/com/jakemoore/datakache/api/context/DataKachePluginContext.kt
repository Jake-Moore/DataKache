package com.jakemoore.datakache.api.context

import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.DataKacheContext
import com.jakemoore.datakache.api.config.DataKachePluginConfig
import com.jakemoore.datakache.api.config.DataKachePluginLang
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.logging.PluginLoggerService
import org.bukkit.configuration.file.FileConfiguration
import org.bukkit.plugin.java.JavaPlugin
import java.io.File

class DataKachePluginContext : DataKacheContext {
    private val plugin: JavaPlugin
    override val logger: LoggerService
    override val config: DataKacheConfig
    override val logFolder: File
    val lang: DataKachePluginLang

    // Primary constructor - only plugin (default)
    constructor(plugin: JavaPlugin) : this(
        plugin = plugin,
        fileConfiguration = DataKachePluginConfig.loadFileConfiguration(
            plugin = plugin,
            resourceFile = "DataKache.yml",
        ),
    )

    constructor(plugin: JavaPlugin, fileConfiguration: FileConfiguration) : this(
        plugin = plugin,
        config = DataKachePluginConfig.loadFromFileConfiguration(fileConfiguration),
        lang = DataKachePluginLang.loadFromFileConfiguration(fileConfiguration)
    )

    // Secondary constructor - plugin + config
    constructor(plugin: JavaPlugin, config: DataKacheConfig, lang: DataKachePluginLang) {
        this.plugin = plugin
        this.logger = PluginLoggerService(plugin)
        this.config = config
        this.logFolder = plugin.dataFolder
        this.lang = lang
    }
}
