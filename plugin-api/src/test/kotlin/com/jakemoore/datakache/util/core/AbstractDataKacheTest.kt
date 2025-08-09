@file:Suppress("unused")

package com.jakemoore.datakache.util.core

import com.jakemoore.datakache.api.mode.StorageMode
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.util.TestPlugin
import com.jakemoore.datakache.util.TestUtil
import com.jakemoore.datakache.util.core.container.DataKacheTestContainer
import com.jakemoore.datakache.util.doc.TestPlayerDocCache
import io.kotest.core.spec.style.DescribeSpec
import kotlinx.serialization.json.Json
import org.bukkit.entity.Player
import org.mockbukkit.mockbukkit.ServerMock
import org.mockbukkit.mockbukkit.entity.PlayerMock

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
    val server: ServerMock
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
     * @return The PlayerMock instance representing the added player
     */
    protected fun addPlayer(playerName: String): PlayerMock = server.addPlayer(playerName)

    /**
     * Gets a player by name from the mock server.
     *
     * @param playerName The name of the player to retrieve
     * @return The PlayerMock instance if found, or null if not found
     */
    protected fun getPlayer(playerName: String): Player? = server.getPlayerExact(playerName)
}
