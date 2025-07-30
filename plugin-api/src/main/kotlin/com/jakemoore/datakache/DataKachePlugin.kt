@file:Suppress("unused")

package com.jakemoore.datakache

import com.jakemoore.datakache.DataKachePlugin.Companion.disableDataKache
import com.jakemoore.datakache.DataKachePlugin.Companion.enableDataKache
import com.jakemoore.datakache.api.context.DataKachePluginContext
import com.jakemoore.datakache.plugin.command.DataKacheCommand
import kotlinx.coroutines.runBlocking
import org.bukkit.Bukkit
import org.bukkit.plugin.java.JavaPlugin

/**
 * Primary [JavaPlugin] class for DataKache.
 *
 * When shading the plugin, the [enableDataKache] and [disableDataKache] methods should be called manually.
 */
class DataKachePlugin : JavaPlugin() {
    override fun onEnable() {
        enableDataKache(this)
    }

    override fun onDisable() {
        disableDataKache(this)
    }

    companion object {
        internal var controller: JavaPlugin? = null

        fun enableDataKache(plugin: JavaPlugin) {
            require(controller == null) {
                "DataKache is already enabled! Please call disableDataKache() before enabling again."
            }

            // Enable DataKache Internals
            runBlocking {
                if (!DataKache.onEnable(DataKachePluginContext(plugin))) {
                    plugin.logger.severe("Failed to enable DataKache! Shutting down...")
                    plugin.server.pluginManager.disablePlugin(plugin)
                    Bukkit.shutdown()
                    return@runBlocking
                }
            }

            // Register Additional Plugin Services
            plugin.getCommand("datakache").executor = DataKacheCommand()

            controller = plugin
        }

        fun disableDataKache(plugin: JavaPlugin) {
            require(controller != null) {
                "DataKache is not enabled! Please call enableDataKache() before disabling."
            }

            // Disable DataKache Internals
            runBlocking {
                if (!DataKache.onDisable()) {
                    plugin.logger.severe("Failed to disable DataKache! Some services may not have shut down properly.")
                }
            }

            controller = null
        }

        fun getController(): JavaPlugin? {
            return controller
        }
    }
}
