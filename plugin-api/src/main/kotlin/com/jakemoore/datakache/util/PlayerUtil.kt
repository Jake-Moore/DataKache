package com.jakemoore.datakache.util

import org.bukkit.Bukkit
import org.bukkit.entity.Player

object PlayerUtil {
    /**
     * @return true IFF [ player != null AND player.isOnline() AND player.isValid() ]
     */
    fun isPlayerOnlineAndAlive(player: Player?): Boolean {
        // isValid checks validity but also if the player is ALIVE
        return player != null && player.isOnline && player.isValid
    }

    /**
     * @return true IFF [ player != null AND player.isOnline() ]
     */
    fun isPlayerOnline(player: Player?): Boolean {
        // checks if the player is still registered to the server (online)
        if (player == null) return false
        return Bukkit.getPlayer(player.uniqueId)?.isOnline ?: false
    }
}
