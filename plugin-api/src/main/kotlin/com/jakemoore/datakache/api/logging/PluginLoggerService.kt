@file:Suppress("unused")

package com.jakemoore.datakache.api.logging

import com.jakemoore.datakache.util.Color
import org.bukkit.Bukkit
import org.bukkit.plugin.java.JavaPlugin

class PluginLoggerService(private val plugin: JavaPlugin, debug: Boolean = true) : LoggerService {
    override val loggerName: String
        get() = "DataKache"
    override val permitsDebugStatements: Boolean = debug

    override fun logToConsole(msg: String, level: LoggerService.LogLevel) {
        val plPrefix = "[$loggerName] "
        when (level) {
            LoggerService.LogLevel.DEBUG -> {
                if (!permitsDebugStatements) return
                Bukkit.getConsoleSender().sendMessage(Color.t("&7[DEBUG] $plPrefix$msg"))
            }
            LoggerService.LogLevel.INFO -> {
                Bukkit.getConsoleSender().sendMessage(Color.t(plPrefix + msg))
            }
            LoggerService.LogLevel.WARNING -> {
                Bukkit.getConsoleSender().sendMessage(Color.t("&e[WARNING] $plPrefix$msg"))
            }
            LoggerService.LogLevel.SEVERE -> {
                Bukkit.getConsoleSender().sendMessage(Color.t("&c[SEVERE] $plPrefix$msg"))
            }
        }
    }
}
