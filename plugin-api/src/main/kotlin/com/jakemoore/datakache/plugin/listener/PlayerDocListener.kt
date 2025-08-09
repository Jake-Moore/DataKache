package com.jakemoore.datakache.plugin.listener

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.DataKachePlugin
import com.jakemoore.datakache.api.DataKacheAPI
import com.jakemoore.datakache.api.cache.PlayerDocCache
import com.jakemoore.datakache.api.doc.PlayerDoc
import com.jakemoore.datakache.api.event.PlayerDocJoinEvent
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.Color
import com.jakemoore.datakache.util.DataKacheFileLogger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.bukkit.Bukkit
import org.bukkit.entity.Player
import org.bukkit.event.EventHandler
import org.bukkit.event.EventPriority
import org.bukkit.event.Listener
import org.bukkit.event.player.AsyncPlayerPreLoginEvent
import org.bukkit.event.player.PlayerJoinEvent
import org.bukkit.event.player.PlayerLoginEvent
import org.bukkit.event.player.PlayerQuitEvent
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.time.TimeSource

/**
 * This Bukkit listener manages [PlayerDoc] objects for joining bukkit [Player]s.
 *
 * This includes:
 * - Creating new [PlayerDoc] objects for new players (on their first join)
 * - Loading existing [PlayerDoc] objects from database (for existing players)
 */
object PlayerDocListener : Listener {
    /**
     * Cache for login permits that are required for a player to pass the Login phase.
     * Without a permit, the player will be denied join.
     * Permits are granted at the end of PreLogin when all PlayerDoc objects are loaded.
     */
    private val loginPermits: Cache<UUID, Boolean> = CacheBuilder.newBuilder()
        .expireAfterWrite(15, TimeUnit.SECONDS)
        .build()

    @EventHandler(priority = EventPriority.LOW)
    fun onPreLogin(event: AsyncPlayerPreLoginEvent) {
        val lang = requireNotNull(DataKachePlugin.context).lang
        val username = event.name
        val uuid = event.uniqueId

        // Deny joins if the database service is not ready to create docs
        if (!DataKache.storageMode.isDatabaseReadyForWrites()) {
            requireNotNull(DataKachePlugin.context).logger.warn(
                "DatabaseService is not ready to write PlayerDocs, denying join for $username ($uuid)."
            )
            event.disallow(
                AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                Color.t(lang.joinDeniedDatabaseNotReady)
            )
            return
        }

        val startMark = TimeSource.Monotonic.markNow()

        // Suspend player join while PlayerDoc's are loaded or created
        val timeoutDuration = lang.preloadPlayerDocTimeout
        val success = runBlocking {
            try {
                return@runBlocking withTimeout(timeoutDuration) {
                    // cache all PlayerDoc objects for this player in parallel
                    return@withTimeout cachePlayerDocs(uuid, username)
                }
            } catch (_: TimeoutCancellationException) {
                val message = lang.joinDeniedPlayerDocTimeout
                requireNotNull(DataKachePlugin.context).logger.warn(
                    "PlayerDoc loading timed out for $username ($uuid) " +
                        "after ${timeoutDuration.inWholeMilliseconds}ms."
                )
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, Color.t(message))
                return@runBlocking false
            } catch (e: Exception) {
                val message = lang.joinDeniedPlayerDocException
                DataKacheFileLogger.severe(
                    "An error occurred while loading PlayerDoc for $username ($uuid): ${e.message}",
                    e
                )
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, Color.t(message))
                return@runBlocking false
            }
        }
        val elapsedMillis = startMark.elapsedNow().inWholeMilliseconds
        if (!success) {
            if (event.loginResult == AsyncPlayerPreLoginEvent.Result.ALLOWED) {
                requireNotNull(DataKachePlugin.context).logger.severe(
                    "Failed to load PlayerDoc for $username ($uuid) " +
                        "after ${elapsedMillis}ms."
                )
                event.disallow(
                    AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                    Color.t(lang.joinDeniedPlayerDocException)
                )
            }
            return // Join was denied, no need to continue
        }

        // Success!
        requireNotNull(DataKachePlugin.context).logger.debug(
            "PlayerDoc for $username ($uuid) loaded in ${elapsedMillis}ms."
        )
        loginPermits.put(uuid, true) // Grant the player a login permit
    }

    @EventHandler(priority = EventPriority.LOW)
    fun onLogin(event: PlayerLoginEvent) {
        val username = event.player.name
        val uuid = event.player.uniqueId

        // Verify that every player has a login permit.
        // This ensures that a player who joined very early during server start-up (and may have skipped pre-login)
        //  is not allowed to join (because their PlayerDocs may not have been created yet).

        if (!loginPermits.asMap().containsKey(uuid)) {
            requireNotNull(DataKachePlugin.context).logger.warn(
                "Player $username ($uuid) connected too early, denying join!"
            )
            val message = requireNotNull(DataKachePlugin.context).lang.joinDeniedEarlyJoin
            event.disallow(PlayerLoginEvent.Result.KICK_OTHER, Color.t(message))
        }

        // Make sure to revoke the login permit after the player has logged in.
        //   This ensures that quick re-joins after a disconnect are not automatically accepted.
        loginPermits.invalidate(event.player.uniqueId)
    }

    @EventHandler(priority = EventPriority.LOWEST)
    fun onJoin(event: PlayerJoinEvent) {
        val player = event.player

        // Join Successful! This join cannot be cancelled anymore.
        // We can now indicate that the player has successfully joined with their PlayerDocs.
        Bukkit.getPluginManager().callEvent(PlayerDocJoinEvent(player))
    }

    @EventHandler(priority = EventPriority.MONITOR)
    fun onQuit(event: PlayerQuitEvent) {
        val player = event.player
        val uuid = player.uniqueId

        // Remove the login permit for this player, they are no longer allowed to join without a new permit.
        loginPermits.invalidate(uuid)
    }

    /**
     * @return if all PlayerDoc objects for the given player were successfully cached.
     */
    private suspend fun cachePlayerDocs(
        uuid: UUID,
        username: String,
    ): Boolean {
        // Wrap each readOrCreate call in an async block to allow parallel loading
        val results = kotlinx.coroutines.coroutineScope {
            DataKacheAPI.listRegistrations()
                .flatMap { it.getDocCaches() }
                .filterIsInstance<PlayerDocCache<*>>()
                .map { cache ->
                    async(Dispatchers.IO) {
                        val createResult = cache.readOrCreate(uuid)
                        // If not successful, return to the caller
                        if (createResult !is Success<PlayerDoc<*>>) {
                            return@async createResult
                        }

                        // If successful, double-check the username field
                        val doc = createResult.value
                        if (doc.username == username) {
                            // username matches, return the normal result
                            return@async createResult
                        }

                        // If the username is not set correctly, update it
                        return@async cache.updateUsername(key = uuid, username)
                    }
                }.awaitAll()
        }

        // Check if all PlayerDoc objects were successfully loaded or created
        results.forEach { result ->
            if (result !is Failure<*>) return@forEach

            DataKacheFileLogger.severe(
                "Failed to load or create PlayerDoc for $username ($uuid): ${result.exception.message}",
                result.exception
            )
        }

        return results.all { it is Success<*> }
    }
}
