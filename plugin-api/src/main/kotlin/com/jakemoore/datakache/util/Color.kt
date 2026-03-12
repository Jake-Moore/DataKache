@file:Suppress("unused")

package com.jakemoore.datakache.util

import org.bukkit.ChatColor
import org.jetbrains.annotations.Contract

object Color {
    @Contract("null -> null")
    fun tNullable(msg: String?): String? {
        if (msg == null) {
            return null
        }
        return ChatColor.translateAlternateColorCodes('&', msg)
    }

    @Suppress("FunctionNameMinLength")
    fun t(msg: String): String = ChatColor.translateAlternateColorCodes('&', msg)
}
