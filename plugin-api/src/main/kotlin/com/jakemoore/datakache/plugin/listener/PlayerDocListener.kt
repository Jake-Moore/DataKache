package com.jakemoore.datakache.plugin.listener

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.DataKachePlugin
import com.jakemoore.datakache.api.DataKacheAPI
import com.jakemoore.datakache.api.cache.PlayerDocCache
import com.jakemoore.datakache.api.doc.PlayerDoc
import com.jakemoore.datakache.api.event.PlayerDocJoinEvent
import com.jakemoore.datakache.util.Color
import com.jakemoore.datakache.util.DataKacheFileLogger
import kotlinx.coroutines.CoroutineScope
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
import kotlin.time.Duration.Companion.milliseconds

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
    private val loginPermits: Cache<UUID, Long> = CacheBuilder.newBuilder()
        .expireAfterWrite(15, TimeUnit.SECONDS)
        .build()

    @EventHandler(priority = EventPriority.LOW)
    fun onPreLogin(event: AsyncPlayerPreLoginEvent) {
        val config = DataKachePlugin.context.fileConfiguration
        val username = event.name
        val uuid = event.uniqueId

        // Deny joins if the database service is not ready to create docs
        if (!DataKache.storageMode.isDatabaseReadyForWrites()) {
            DataKachePlugin.context.logger.warn(
                "DatabaseService is not ready to write PlayerDocs, denying join for $username ($uuid)."
            )
            val message = config.getString(
                "language.joinDenied.databaseNotReady",
                "&cConnection failed."
            )
            event.disallow(
                AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                Color.t(message)
            )
            return
        }

        val msStart = System.currentTimeMillis()

        // Suspend player join while PlayerDoc's are loaded or created
        val timeoutMS = config.getInt("joinOptions.preloadPlayerDocTimeoutMS", 5_000)
        val success = runBlocking {
            try {
                withTimeout(timeoutMS.milliseconds) {
                    // cache all PlayerDoc objects for this player in parallel
                    cachePlayerDocs(uuid, username)
                }
                return@runBlocking true
            } catch (_: TimeoutCancellationException) {
                val message = config.getString(
                    "language.joinDenied.playerDocTimeout",
                    "&cConnection failed."
                )
                DataKachePlugin.context.logger.warn(
                    "PlayerDoc loading timed out for $username ($uuid) after ${timeoutMS}ms."
                )
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, Color.t(message))
                return@runBlocking false
            } catch (e: Exception) {
                val message = config.getString(
                    "language.joinDenied.playerDocException",
                    "&cConnection failed."
                )
                DataKacheFileLogger.severe(
                    "An error occurred while loading PlayerDoc for $username ($uuid): ${e.message}",
                    e
                )
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, Color.t(message))
                return@runBlocking false
            }
        }
        if (!success) {
            return // Join was denied, no need to continue
        }

        // Success!
        DataKachePlugin.context.logger.debug(
            "PlayerDoc for $username ($uuid) loaded in ${System.currentTimeMillis() - msStart}ms."
        )
        loginPermits.put(uuid, System.currentTimeMillis()) // Grant the player a login permit
    }

    @EventHandler(priority = EventPriority.LOW)
    fun onLogin(event: PlayerLoginEvent) {
        val username = event.player.name
        val uuid = event.player.uniqueId

        // Verify that every player has a login permit.
        // This ensures that a player who joined very early during server start-up (and may have skipped pre-login)
        //  is not allowed to join (because their PlayerDocs may not have been created yet).

        if (!loginPermits.asMap().containsKey(uuid)) {
            DataKachePlugin.context.logger.warn(
                "Player $username ($uuid) connected too early, denying join!"
            )
            val message = DataKachePlugin.context.fileConfiguration.getString(
                "language.joinDenied.earlyJoin",
                "&cConnection failed."
            )
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

    private suspend fun cachePlayerDocs(
        uuid: UUID,
        username: String,
    ) {
        // Wrap each readOrCreate call in an async block to allow parallel loading
        val loadsDeferred = DataKacheAPI.listRegistrations()
            .flatMap { it.getDocCaches() }
            .filterIsInstance(PlayerDocCache::class.java)
            .map { cache ->
                CoroutineScope(Dispatchers.IO).async {
                    cache.readOrCreate(uuid) { doc ->
                        doc.copyHelper(username = username)
                    }
                }
            }

        // Wait for all loads to complete
        loadsDeferred.awaitAll()
    }
}
