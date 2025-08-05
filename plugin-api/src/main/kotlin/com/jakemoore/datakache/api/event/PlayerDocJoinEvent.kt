package com.jakemoore.datakache.api.event

import org.bukkit.entity.Player
import org.bukkit.event.Event
import org.bukkit.event.HandlerList

/**
 * This event is called during the [org.bukkit.event.player.PlayerJoinEvent].
 *
 * It guarantees that all [com.jakemoore.datakache.api.doc.PlayerDoc] objects for the player have been loaded.
 */
@Suppress("unused")
class PlayerDocJoinEvent(
    val player: Player,
) : Event() {
    override fun getHandlers(): HandlerList {
        return handlerList
    }

    companion object {
        @JvmStatic
        val handlerList = HandlerList()
    }
}
