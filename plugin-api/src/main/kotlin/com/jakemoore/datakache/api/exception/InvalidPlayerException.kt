package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.exception.data.Operation
import org.bukkit.entity.Player

class InvalidPlayerException(
    /**
     * The invalid player instance sent to the operation.
     */
    val player: Player,
    val operation: Operation,
) : DataKacheException(
    "Cannot ${operation.name}: Player '${player.name}' " +
        "(UUID: ${player.uniqueId}) is not online or valid."
)
