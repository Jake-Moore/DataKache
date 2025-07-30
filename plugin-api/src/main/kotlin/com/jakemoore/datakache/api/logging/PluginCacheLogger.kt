package com.jakemoore.datakache.api.logging

import org.bukkit.plugin.java.JavaPlugin

open class PluginCacheLogger(
    protected val cacheName: String,
    protected val plugin: JavaPlugin,
) : PluginLoggerService(plugin) {
    override val loggerName: String
        get() = "C: $cacheName"
}
