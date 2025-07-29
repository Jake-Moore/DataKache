@file:Suppress("unused")

package com.jakemoore.datakache

import com.jakemoore.datakache.api.context.DataKachePluginContext
import com.jakemoore.datakache.plugin.command.DataKacheCommand
import kotlinx.coroutines.runBlocking
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
        private var controller: JavaPlugin? = null

        fun enableDataKache(plugin: JavaPlugin) {
            require(controller == null) {
                "DataKache is already enabled! Please call disableDataKache() before enabling again."
            }

            // Enable DataKache Internals
            runBlocking {
                DataKache.onEnable(DataKachePluginContext(plugin))
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
                DataKache.onDisable()
            }

            controller = null
        }

        fun getController(): JavaPlugin? {
            return controller
        }
    }
}
