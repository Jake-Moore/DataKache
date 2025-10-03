package com.jakemoore.datakache.api.context

import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.DataKacheContext
import com.jakemoore.datakache.api.config.DataKachePluginConfig
import com.jakemoore.datakache.api.config.DataKachePluginLang
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.logging.PluginLoggerService
import org.bukkit.plugin.java.JavaPlugin
import java.io.File

class DataKachePluginContext : DataKacheContext {
    private val plugin: JavaPlugin
    override val logger: LoggerService
    override val config: DataKacheConfig
    override val logFolder: File
    val lang: DataKachePluginLang

    constructor(plugin: JavaPlugin) : this(
        plugin = plugin,
        config = DataKachePluginConfig.loadDataKacheConfig(plugin),
        lang = DataKachePluginConfig.loadDataKacheLang(plugin)
    )

    constructor(plugin: JavaPlugin, config: DataKacheConfig, lang: DataKachePluginLang) {
        this.plugin = plugin
        this.logger = PluginLoggerService(plugin)
        this.config = config
        this.logFolder = plugin.dataFolder.also { it.mkdirs() }
        this.lang = lang
    }
}
