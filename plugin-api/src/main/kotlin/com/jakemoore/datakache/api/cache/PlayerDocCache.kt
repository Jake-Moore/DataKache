@file:Suppress("unused")

package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.api.doc.PlayerDoc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.exception.InvalidPlayerException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentKeyModificationException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentUsernameModificationException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentVersionModificationException
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
import com.jakemoore.datakache.api.result.handler.RejectableUpdatePlayerDocResultHandler
import com.jakemoore.datakache.api.result.handler.UpdatePlayerDocResultHandler
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
        if (!PlayerUtil.isFullyValidPlayer(player)) {
            throw InvalidPlayerException(
                player = player,
                operation = "read",
            )
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
                        createAndInsertNewPlayerDoc(
                            uuid = player.uniqueId,
                            username = player.name,
                            loginEvent = null,
                            initializer = { it },
                        )
                    }
                }
            }
        }
    }

    override suspend fun create(key: UUID, initializer: (D) -> D): DefiniteResult<D> {
        return CreatePlayerDocResultHandler.wrap {
            // Create a new instance in modifiable state
            return@wrap createAndInsertNewPlayerDoc(
                uuid = key,
                username = null,
                loginEvent = null,
                initializer = initializer,
            )
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

    // Regular update method (does not bypass validation)
    override suspend fun update(key: UUID, updateFunction: (D) -> D): DefiniteResult<D> {
        return UpdatePlayerDocResultHandler.wrap {
            return@wrap updateInternal(key, updateFunction, false)
        }
    }

    // Regular update (rejectable) method (does not bypass validation)
    override suspend fun updateRejectable(key: UUID, updateFunction: (D) -> D): RejectableResult<D> {
        return RejectableUpdatePlayerDocResultHandler.wrap {
            return@wrap updateInternal(key, updateFunction, false)
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
        if (!PlayerUtil.isFullyValidPlayer(player)) {
            throw InvalidPlayerException(
                player = player,
                operation = "update",
            )
        }
        return this.update(player.uniqueId, updateFunction)
    }

    /**
     * Update the username of the [PlayerDoc] associated with the provided [Player].
     *
     * This method is intended for internal use only, as it modifies the username directly.
     *
     * API users cannot modify the username of a [PlayerDoc] during creation or updates, that will throw:
     * - [IllegalDocumentUsernameModificationException] if attempted.
     */
    @ApiStatus.Internal
    internal suspend fun updateUsername(
        key: UUID,
        username: String,
    ): DefiniteResult<D> {
        return UpdatePlayerDocResultHandler.wrap {
            return@wrap updateInternal(
                key = key,
                updateFunction = {
                    it.copyHelper(username = username)
                },
                bypassValidation = true, // Bypass validation to allow username modification
            )
        }
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
        if (!PlayerUtil.isFullyValidPlayer(player)) {
            throw InvalidPlayerException(
                player = player,
                operation = "updateRejectable",
            )
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
            val defaultDoc = constructNewPlayerDoc(key, username) { it }

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
        if (!PlayerUtil.isFullyValidPlayer(player)) {
            throw InvalidPlayerException(
                player = player,
                operation = "delete",
            )
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
    private suspend fun createAndInsertNewPlayerDoc(
        uuid: UUID,
        username: String? = null,
        loginEvent: AsyncPlayerPreLoginEvent? = null,
        initializer: (D) -> D,
    ): D {
        val doc: D = constructNewPlayerDoc(uuid, username, initializer)

        // Access internal method to save and cache the document
        return this.insertDocumentInternal(doc, force = true)
    }

    private fun constructNewPlayerDoc(
        uuid: UUID,
        username: String?,
        initializer: (D) -> D,
    ): D {
        val namespace = this.getKeyNamespace(uuid)

        // Create from instantiator
        val instantiated: D = this.instantiator(uuid, 0L, username)
        instantiated.initializeInternal(this)

        // Initialize the document with default values
        val default: D = this.defaultInitializer(instantiated)
        validateInitializer(namespace, uuid, 0L, username, default)

        // Initialize with custom initializer
        val doc: D = initializer(default)
        validateInitializer(namespace, uuid, 0L, username, doc)

        doc.initializeInternal(this)
        return doc
    }

    @Throws(IllegalDocumentKeyModificationException::class, IllegalDocumentVersionModificationException::class)
    @Suppress("SameParameterValue")
    private fun validateInitializer(
        namespace: String,
        expectedKey: UUID,
        expectedVersion: Long,
        expectedUsername: String?,
        doc: D,
    ) {
        // Require the Key to stay the same
        if (doc.key != expectedKey) {
            val foundKeyString = this.keyToString(doc.key)
            val expectedKeyString = this.keyToString(expectedKey)
            throw IllegalDocumentKeyModificationException(
                namespace,
                foundKeyString,
                expectedKeyString,
            )
        }

        // Require the Version to stay the same
        val foundVersion = doc.version
        if (foundVersion != expectedVersion) {
            throw IllegalDocumentVersionModificationException(namespace, foundVersion, expectedVersion)
        }

        // Require the Username to stay the same
        val foundUsername = doc.username
        if (foundUsername != expectedUsername) {
            throw IllegalDocumentUsernameModificationException(
                docNamespace = namespace,
                foundUsername = foundUsername,
                expectedUsername = expectedUsername,
            )
        }
    }

    override fun isUpdateValidInternal(originalDoc: D, updatedDoc: D) {
        val namespace = this.getKeyNamespace(originalDoc.key)

        // Enforce Username immutability and internal modification ONLY
        val originalUsername = originalDoc.username
        val updatedUsername = updatedDoc.username
        if (originalUsername != updatedUsername) {
            throw IllegalDocumentUsernameModificationException(
                docNamespace = namespace,
                expectedUsername = originalUsername,
                foundUsername = updatedUsername,
            )
        }
    }
}
