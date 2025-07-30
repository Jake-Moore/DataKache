@file:Suppress("unused")

package com.jakemoore.datakache.api.extension

import com.jakemoore.datakache.DataKachePlugin
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.client.PluginKacheClient
import org.bukkit.Bukkit
import org.bukkit.plugin.IllegalPluginAccessException

/**
 * Helper method to run a [Runnable] asynchronously on the server (task registered via cache's client).
 */
fun DocCache<*, *>.runAsync(runnable: Runnable) {
    val client = this.registration.client
    val plugin = if (client is PluginKacheClient) {
        client.plugin
    } else {
        requireNotNull(DataKachePlugin.controller) {
            "Unable to schedule async task: DataKachePlugin is not initialized or controller is null."
        }
    }
    Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable)
}

/**
 * Helper method to run a [Runnable] synchronously on the server (task registered via cache's client).
 * This is useful for tasks that need to be executed on the main server thread.
 */
fun DocCache<*, *>.runSync(runnable: Runnable) {
    val client = this.registration.client
    val plugin = if (client is PluginKacheClient) {
        client.plugin
    } else {
        requireNotNull(DataKachePlugin.controller) {
            "Unable to schedule sync task: DataKachePlugin is not initialized or controller is null."
        }
    }
    Bukkit.getScheduler().runTask(plugin, runnable)
}

/**
 * Helper method to attempt to run a [Runnable] asynchronously.
 * If the plugin is not allowed to run async tasks (like on disable), a sync task will be run instead.
 */
fun DocCache<*, *>.tryAsync(runnable: Runnable) {
    try {
        runAsync(runnable)
    } catch (e: IllegalPluginAccessException) {
        runSync(runnable)
    }
}
