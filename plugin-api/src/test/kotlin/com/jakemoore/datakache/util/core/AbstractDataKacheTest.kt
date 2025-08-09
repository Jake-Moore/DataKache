@file:Suppress("unused")

package com.jakemoore.datakache.util.core

import com.jakemoore.datakache.api.mode.StorageMode
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.util.TestPlugin
import com.jakemoore.datakache.util.TestUtil
import com.jakemoore.datakache.util.core.container.DataKacheTestContainer
import com.jakemoore.datakache.util.doc.TestPlayerDocCache
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.spec.style.DescribeSpec
import kotlinx.serialization.json.Json
import org.bukkit.entity.Player
import org.mockbukkit.mockbukkit.ServerMock
import org.mockbukkit.mockbukkit.entity.PlayerMock
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

/**
 * Abstract base class for DataKache integration tests.
 *
 * Provides common setup and teardown functionality for all tests.
 */
abstract class AbstractDataKacheTest : DescribeSpec() {

    protected val json = Json {
        encodeDefaults = true // Encodes default data class property values (instead of omitting them)
        explicitNulls = true // Encodes null values (instead of omitting them)
        prettyPrint = true
    }

    private lateinit var testContainer: DataKacheTestContainer

    init {
        beforeSpec {
            // TODO figure out a way to test all storage modes instead of just MongoDB
            //  probably by using an environment variable or system property to set the mode
            testContainer = TestUtil.createTestContainer(StorageMode.MONGODB)
            testContainer.beforeSpec()
        }

        beforeEach {
            testContainer.beforeEach()
        }

        afterEach {
            testContainer.afterEach()
        }

        afterSpec {
            testContainer.afterSpec()
        }
    }

    /**
     * The [TestPlayerDocCache] instance for testing.
     */
    val cache: TestPlayerDocCache
        get() = testContainer.cache

    /**
     * The [DataKacheRegistration] instance for testing.
     */
    val registration: DataKacheRegistration
        get() = testContainer.registration

    /**
     * The [ServerMock] instance for testing.
     */
    private val server: ServerMock
        get() = testContainer.server

    /**
     * The [TestPlugin] instance for testing.
     */
    val plugin: TestPlugin
        get() = testContainer.plugin

    /**
     * Adds a player to the mock server.
     *
     * @param playerName The name of the player to add
     * @return The [PlayerMock] instance representing the added player
     */
    protected suspend fun addPlayer(playerName: String): PlayerMock {
        return eventually(5.seconds) {
            server.addPlayer(playerName)
            return@eventually checkMockPlayer(playerName)
        }
    }

    /**
     * Adds a player to the mock server.
     *
     * @param playerName The name of the player to add
     * @return The [PlayerMock] instance representing the added player
     */
    protected suspend fun addPlayer(playerName: String, uuid: UUID): PlayerMock {
        return addPlayer(makePlayer(playerName, uuid))
    }

    /**
     * Adds a player to the mock server.
     *
     * @param player The [PlayerMock] instance to add
     * @return The [PlayerMock] instance representing the added player
     */
    protected suspend fun addPlayer(player: PlayerMock): PlayerMock {
        return eventually(5.seconds) {
            server.addPlayer(player)
            return@eventually checkMockPlayer(player.name)
        }
    }

    private fun checkMockPlayer(playerName: String): PlayerMock {
        // Ensure the login went through
        val p2 = server.getPlayerExact(playerName)
            ?: error("Player $playerName not found after adding")
        require(p2.isOnline) {
            "Player $playerName is not online after adding"
        }
        require(p2.isValid) {
            "Player $playerName is not valid after adding"
        }

        return p2 as? PlayerMock ?: error("Player $playerName is not a PlayerMock")
    }

    /**
     * Creates a new player with the given name and UUID.
     *
     * This player still needs to be added to the server using [addPlayer]
     *
     * @param playerName The name of the player to create
     * @param uuid The UUID of the player (defaults to a random UUID)
     * @return The [PlayerMock] instance representing the created player
     */
    protected fun makePlayer(
        playerName: String,
        uuid: UUID = UUID.randomUUID(),
    ): PlayerMock {
        return PlayerMock(server, playerName, uuid)
    }

    /**
     * Gets a player by name from the mock server.
     *
     * @param playerName The name of the player to retrieve
     * @return The [Player] instance if found, or null if not found
     */
    protected fun getPlayerExact(playerName: String): Player? = server.getPlayerExact(playerName)
}
