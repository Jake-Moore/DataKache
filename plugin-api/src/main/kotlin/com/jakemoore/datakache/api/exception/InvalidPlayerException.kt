@file:Suppress("unused")

package com.jakemoore.datakache.api.exception

import org.bukkit.entity.Player

class InvalidPlayerException(
    val player: Player,
    val operation: String,
) : DataKacheException(
    "Player '${player.name}' (UUID: ${player.uniqueId}) is not online or valid."
)
