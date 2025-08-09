package com.jakemoore.datakache.util

import org.bukkit.plugin.java.JavaPlugin

/**
 * Open test plugin for MockBukkit to use.
 *
 * Needs to be open so that MockBukkit can subclass it.
 */
open class TestPlugin : JavaPlugin() {
    override fun onEnable() {
        println("TestPlugin enabled")
    }

    override fun onDisable() {
        println("TestPlugin disabled")
    }
}
