@file:Suppress("unused")

package com.jakemoore.datakache.api.exception

import org.bukkit.entity.Player

class InvalidPlayerException(
    /**
     * The invalid player instance sent to the operation.
     */
    val player: Player,
    val operation: String,
) : DataKacheException(
    "Cannot $operation: Player '${player.name}' (UUID: ${player.uniqueId}) is not online or valid."
)
