@file:Suppress("unused")

package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.api.doc.PlayerDoc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.logging.PluginCacheLogger
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.RejectableResult
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.handler.ClearPlayerDocResultHandler
import com.jakemoore.datakache.api.result.handler.CreatePlayerDocResultHandler
import com.jakemoore.datakache.util.DataKacheFileLogger
import com.jakemoore.datakache.util.PlayerUtil
import kotlinx.coroutines.runBlocking
import org.bukkit.Bukkit
import org.bukkit.entity.Player
import org.bukkit.event.player.AsyncPlayerPreLoginEvent
import org.bukkit.plugin.java.JavaPlugin
import org.jetbrains.annotations.ApiStatus
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
     * @param String? the username (if known) of the player.
     */
    val instantiator: (UUID, Long, String?) -> D,

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
        // Nothing to do here, no special shutdown logic for GenericDocCache
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
        require(PlayerUtil.isFullyValidPlayer(player)) {
            "Cannot read PlayerDoc for player ${player.name} (${player.uniqueId}). Player is not online or valid!"
        }

        return when (val result = this.read(player.uniqueId)) {
            is Success, is Failure -> {
                // On Success -> return document
                // On Failure -> pass failure through
                result
            }
            is Empty -> {
                // PANIC! The provided player is online, but the PlayerDoc is not present in the cache.
                // This should never happen, as DataKache ensures that a PlayerDoc is created for
                // every online player.
                DataKacheFileLogger.severe(
                    "[PlayerDocCache#read] PlayerDoc for player ${player.name} (${player.uniqueId})" +
                        " is not cached. This should not happen! Attempting to resolve (MAY LAG!)."
                )

                // Best solution - create a new PlayerDoc using basic initialization, and any future updates
                //  will handle any broken optimistic versioning or caching behavior.
                return runBlocking {
                    CreatePlayerDocResultHandler.wrap {
                        // method will also save the document to database and cache
                        createNewPlayerDoc(player.uniqueId, player.name, null)
                    }
                }
            }
        }
    }

    override suspend fun create(key: UUID, initializer: (D) -> D): DefiniteResult<D> {
        return CreatePlayerDocResultHandler.wrap {
            // Create a new instance in modifiable state
            val instantiated: D = instantiator(key, 0L, null)
            instantiated.initializeInternal(this)

            // Allow caller to initialize the document with starter data
            val doc: D = initializer(instantiated)
            require(doc.key == key) {
                "The key of the PlayerDoc must not change during initializer. Expected: $key, Actual: ${doc.key}"
            }
            assert(doc.version == 0L) {
                "The version of the PlayerDoc must not change during initializer. Expected: 0L, Actual: ${doc.version}"
            }
            doc.initializeInternal(this)

            // Access internal method to save and cache the document
            return@wrap this.insertDocumentInternal(doc)
        }
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

    /**
     * Modify the document associated with the provided [Player] (both cache and database will be updated).
     *
     * @param player The **online** player who owns the document to be modified (uses Player UUID).
     *
     * @return A [DefiniteResult] containing the updated document, or an exception if the document could not be updated.
     */
    suspend fun update(player: Player, updateFunction: (D) -> D): DefiniteResult<D> {
        require(PlayerUtil.isFullyValidPlayer(player)) {
            "Cannot update PlayerDoc for player ${player.name} (${player.uniqueId}). Player is not online or valid!"
        }
        return this.update(player.uniqueId, updateFunction)
    }

    /**
     * Modify a document associated with the provided [Player],
     *   allowing the operation to gracefully be rejected within the [updateFunction].
     *
     * @param player The **online** player who owns the document to be modified (uses Player UUID).
     *
     * Within the [updateFunction], you can throw a [RejectUpdateException] to cancel the update operation. The
     * [RejectableResult] will then indicate that the update was rejected, and no modifications were made.
     *
     * @return The [RejectableResult] containing:
     * - the updated document if the update was successful
     * - an exception if the update failed
     * - or a rejection state if the update was rejected by the [updateFunction]
     */
    @Throws(DocumentNotFoundException::class)
    suspend fun updateRejectable(player: Player, updateFunction: (D) -> D): RejectableResult<D> {
        require(PlayerUtil.isFullyValidPlayer(player)) {
            "Cannot update PlayerDoc for player ${player.name} (${player.uniqueId}). Player is not online or valid!"
        }
        return this.updateRejectable(player.uniqueId, updateFunction)
    }

    /**
     * **Clears** all document data for the [PlayerDoc] from the cache and the backing database.
     *
     * This is NOT a traditional deletion, as we must ensure that all [Player]s have a [PlayerDoc] present,
     * so this method will reset the document to its default state from [defaultInitializer].
     *
     * @param key The unique key of the document to be cleared.
     *
     * @return A [DefiniteResult] indicating if the document was found and cleared. (false = not found)
     */
    override suspend fun delete(key: UUID): DefiniteResult<Boolean> {
        return ClearPlayerDocResultHandler.wrap {
            val username: String? = this@PlayerDocCache.read(key).getOrNull()?.username
            val defaultDoc = constructNewPlayerDoc(key, username)

            // Replace the current document with the new one.
            // If a DocumentNotFoundException is thrown, it means the document was not found.
            //  which in our case is acceptable, it just means the caller didn't realize the document was not present.
            try {
                this.replaceDocumentInternal(key, defaultDoc)
                return@wrap true
            } catch (_: DocumentNotFoundException) {
                return@wrap false
            }
        }
    }

    /**
     * Deletes a document from the cache and the backing database.
     *
     * @param player The **online** player who owns the document to be deleted (uses Player UUID).
     *
     * @return A [DefiniteResult] indicating if the document was found and deleted. (false = not found)
     */
    suspend fun delete(player: Player): DefiniteResult<Boolean> {
        require(PlayerUtil.isFullyValidPlayer(player)) {
            "Cannot delete PlayerDoc for player ${player.name} (${player.uniqueId}). Player is not online or valid!"
        }
        return this.delete(player.uniqueId)
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

    // ------------------------------------------------------------ //
    //                        Internal Methods                      //
    // ------------------------------------------------------------ //
    @ApiStatus.Internal
    @Throws(DuplicateDocumentKeyException::class, DuplicateUniqueIndexException::class)
    private suspend fun createNewPlayerDoc(
        uuid: UUID,
        username: String? = null,
        loginEvent: AsyncPlayerPreLoginEvent? = null,
    ): D {
        val doc: D = constructNewPlayerDoc(uuid, username)

        // Access internal method to save and cache the document
        return this.insertDocumentInternal(doc)
    }

    private fun constructNewPlayerDoc(uuid: UUID, username: String?): D {
        // Create from instantiator
        val instantiated: D = this.instantiator(uuid, 0L, username)
        instantiated.initializeInternal(this)

        // Initialize the document with default values
        val doc: D = this.defaultInitializer(instantiated)
        require(doc.key == uuid) {
            "The key of the PlayerDoc must not change during initializer. Expected: $uuid, Actual: ${doc.key}"
        }
        assert(doc.version == 0L) {
            "The version of the PlayerDoc must not change during initializer. Expected: 0L, Actual: ${doc.version}"
        }
        doc.initializeInternal(this)
        return doc
    }
}
