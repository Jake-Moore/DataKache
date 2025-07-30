@file:Suppress("unused")

package com.jakemoore.datakache.api.client

import com.jakemoore.datakache.api.DataKacheClient
import org.bukkit.plugin.java.JavaPlugin

class PluginKacheClient(
    val plugin: JavaPlugin,
) : DataKacheClient {
    override val name: String
        get() = plugin.name
}
