@file:Suppress("unused")

package com.jakemoore.datakache

import com.jakemoore.datakache.DataKachePlugin.Companion.disableDataKache
import com.jakemoore.datakache.DataKachePlugin.Companion.enableDataKache
import com.jakemoore.datakache.api.context.DataKachePluginContext
import com.jakemoore.datakache.plugin.command.DataKacheCommand
import com.jakemoore.datakache.plugin.listener.PlayerDocListener
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
        private var controller: JavaPlugin? = null
        internal lateinit var context: DataKachePluginContext

        /**
         * @return true if DataKache was successfully enabled, false otherwise.
         */
        fun enableDataKache(plugin: JavaPlugin, ctx: DataKachePluginContext? = null): Boolean {
            require(controller == null) {
                "DataKache is already enabled! Please call disableDataKache() before enabling again."
            }
            context = ctx ?: DataKachePluginContext(plugin)

            // Enable DataKache Internals
            val success = runBlocking {
                if (!DataKache.onEnable(context)) {
                    plugin.logger.severe("Failed to enable DataKache! Shutting down...")
                    plugin.server.pluginManager.disablePlugin(plugin)
                    Bukkit.shutdown()
                    return@runBlocking false
                }
                return@runBlocking true
            }
            if (!success) return false

            // Register Additional Plugin Services
            plugin.getCommand("datakache").executor = DataKacheCommand()

            // Register PlayerDocListener
            Bukkit.getPluginManager().registerEvents(PlayerDocListener, plugin)

            controller = plugin
            return true
        }

        /**
         * @return true if DataKache was successfully disabled, false otherwise.
         */
        fun disableDataKache(plugin: JavaPlugin): Boolean {
            require(controller != null) {
                "DataKache is not enabled! Please call enableDataKache() before disabling."
            }

            // Disable DataKache Internals
            val success = runBlocking {
                if (!DataKache.onDisable()) {
                    plugin.logger.severe("Failed to disable DataKache! Some services may not have shut down properly.")
                    return@runBlocking false
                } else {
                    return@runBlocking true
                }
            }

            controller = null
            return success
        }

        fun getController(): JavaPlugin? {
            return controller
        }
    }
}
