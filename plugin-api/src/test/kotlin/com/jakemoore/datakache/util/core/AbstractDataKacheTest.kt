@file:Suppress("unused")

package com.jakemoore.datakache.util.core

import com.jakemoore.datakache.api.mode.StorageMode
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.util.TestPlugin
import com.jakemoore.datakache.util.TestUtil
import com.jakemoore.datakache.util.core.container.DataKacheTestContainer
import com.jakemoore.datakache.util.doc.TestPlayerDocCache
import io.kotest.core.spec.style.DescribeSpec
import kotlinx.coroutines.runBlocking
import org.bukkit.entity.Player
import org.mockbukkit.mockbukkit.ServerMock
import org.mockbukkit.mockbukkit.entity.PlayerMock

/**
 * Abstract base class for DataKache integration tests.
 *
 * Provides common setup and teardown functionality for all tests.
 */
abstract class AbstractDataKacheTest : DescribeSpec() {

    private lateinit var testContainer: DataKacheTestContainer

    init {
        beforeSpec {
            runBlocking {
                // TODO figure out a way to test all storage modes instead of just MongoDB
                testContainer = TestUtil.createTestContainer(StorageMode.MONGODB)
                testContainer.beforeSpec()
            }
        }

        beforeEach {
            runBlocking {
                testContainer.beforeEach()
            }
        }

        afterEach {
            runBlocking {
                testContainer.afterEach()
            }
        }

        afterSpec {
            runBlocking {
                testContainer.afterSpec()
            }
        }
    }

    /**
     * Gets the test cache instance.
     *
     * @return The TestPlayerDocCache instance for this test
     */
    protected fun getCache(): TestPlayerDocCache = testContainer.getCache()

    /**
     * Gets the mock server instance.
     */
    protected fun getServer(): ServerMock = testContainer.getServer()

    /**
     * Gets the mock plugin instance.
     */
    protected fun getPlugin(): TestPlugin = testContainer.getPlugin()

    /**
     * Adds a player to the mock server.
     *
     * @param playerName The name of the player to add
     * @return The PlayerMock instance representing the added player
     */
    protected fun addPlayer(playerName: String): PlayerMock = getServer().addPlayer(playerName)

    /**
     * Gets a player by name from the mock server.
     *
     * @param playerName The name of the player to retrieve
     * @return The PlayerMock instance if found, or null if not found
     */
    protected fun getPlayer(playerName: String): Player? = getServer().getPlayerExact(playerName)

    /**
     * Gets the DataKache registration instance.
     *
     * @return The DataKacheRegistration instance for this test
     */
    protected fun getRegistration(): DataKacheRegistration = testContainer.getRegistration()
}
