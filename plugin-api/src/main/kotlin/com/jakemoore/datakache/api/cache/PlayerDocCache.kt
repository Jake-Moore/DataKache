@file:Suppress("unused")

package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.api.doc.PlayerDoc
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.logging.PluginCacheLogger
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.util.DataKacheFileLogger
import com.jakemoore.datakache.util.PlayerUtil
import org.bukkit.Bukkit
import org.bukkit.entity.Player
import org.bukkit.plugin.java.JavaPlugin
import java.util.UUID
import kotlin.reflect.KProperty

abstract class PlayerDocCache<D : PlayerDoc<D>>(
    plugin: JavaPlugin,

    cacheName: String,
    registration: DataKacheRegistration,
    docClass: Class<D>,
    logger: (String) -> LoggerService = { cacheName -> PluginCacheLogger(cacheName, plugin) },

    /**
     * @param UUID the unique identifier for the document.
     * @param Long the version of the document.
     * @param String the username of the player.
     */
    val instantiator: (UUID, Long, String) -> D,

    /**
     * [PlayerDoc] documents are created automatically when a player joins the server. You cannot create them manually.
     *
     * As such, the traditional 'initializer' is unavailable, and is therefore supplied here.
     *
     * When each [PlayerDoc] for this cache is created, this initializer will be used to set the initial values.
     */
    val defaultInitializer: (D) -> D,

) : DocCacheImpl<UUID, D>(cacheName, registration, docClass, logger) {
    // ------------------------------------------------------------ //
    //                     Kotlin Reflect Access                    //
    // ------------------------------------------------------------ //
    abstract fun getUsernameKProperty(): KProperty<String?>


    // ------------------------------------------------------------ //
    //                         Service Methods                      //
    // ------------------------------------------------------------ //
    override suspend fun shutdownSuper(): Boolean {
        Bukkit.getOnlinePlayers().forEach { p: Player ->
            // TODO need to ensure that every player is quit from the cache on shutdown
            // PlayerDocListener.quit(p, this)
        }
        return true
    }



    // ------------------------------------------------------------ //
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //
    /**
     * DataKache will ensure that a [PlayerDoc] is present for every online [Player].
     *
     * Therefore, it is possible to read a [PlayerDoc] from the cache without querying the database.
     *
     * @param player The online player whose document is to be read.
     *
     * @return An [DefiniteResult] containing the document or an exception if the document could not be read.
     */
    fun read(player: Player): DefiniteResult<D> {
        TODO() // TODO
    }

    /**
     * Fetches all [PlayerDoc] objects for all online [Player]s.
     *
     * @return A list of [PlayerDoc] objects for all currently online players.
     */
    fun readAllOnline(): List<D> {
        return Bukkit.getOnlinePlayers()
            .filter { PlayerUtil.isFullyValidPlayer(it) }
            .mapNotNull { player ->
                val result = this.read(player)
                val exception = result.exceptionOrNull()
                if (exception != null) {
                    DataKacheFileLogger.warn(
                        "[PlayerDocCache#readAllOnline] Failed to read PlayerDoc for " +
                                "player ${player.name} (${player.uniqueId}). " +
                                "This should not happen, please report this issue to the DataKache team.",
                        exception
                    )
                    return@mapNotNull null
                }

                this.read(player).getOrNull()
            }
    }



    // ------------------------------------------------------------ //
    //                    Key Manipulation Methods                  //
    // ------------------------------------------------------------ //
    override fun keyFromString(string: String): UUID {
        return UUID.fromString(string)
    }
    override fun keyToString(key: UUID): String {
        return key.toString()
    }
}
