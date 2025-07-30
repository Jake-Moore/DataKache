package com.jakemoore.datakache.util

import org.bukkit.entity.Player

object PlayerUtil {
    /**
     * @return true IFF [ player != null AND player.isOnline() AND player.isValid() ]
     */
    fun isFullyValidPlayer(player: Player?): Boolean {
        return player != null && player.isOnline && player.isValid
    }
}
