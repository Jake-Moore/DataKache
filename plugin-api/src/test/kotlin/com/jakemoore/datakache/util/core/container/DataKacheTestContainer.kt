package com.jakemoore.datakache.util.core.container

import com.jakemoore.datakache.DataKachePlugin
import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.util.TestPlugin
import com.jakemoore.datakache.util.doc.TestPlayerDocCache
import org.mockbukkit.mockbukkit.ServerMock

/**
 * Abstract container for managing DataKache test infrastructure.
 *
 * This interface provides a database-agnostic way to manage test containers,
 * including setup, teardown, and access to test resources.
 */
interface DataKacheTestContainer {

    /**
     * Starts the test container and prepares the environment.
     *
     * Should be called from [io.kotest.core.spec.Spec.beforeSpec]
     */
    suspend fun beforeSpec()

    /**
     * Cleans up the test container and releases resources.
     *
     * Should be called from [io.kotest.core.spec.Spec.afterSpec]
     */
    suspend fun afterSpec()

    /**
     * Performs any setup required before each test.
     *
     * Should be called from [io.kotest.core.spec.Spec.beforeEach]
     */
    suspend fun beforeEach()

    /**
     * Performs any cleanup required after each test.
     *
     * Should be called from [io.kotest.core.spec.Spec.afterEach]
     */
    suspend fun afterEach()

    /**
     * Gets the test cache instance.
     *
     * @return The TestPlayerDocCache instance for this test container
     */
    fun getCache(): TestPlayerDocCache

    /**
     * Gets the mock server instance for this test container.
     *
     * @return The [ServerMock] instance for this test container
     */
    fun getServer(): ServerMock

    /**
     * Gets the mock plugin instance for this test container.
     *
     * @return The [DataKachePlugin] instance for this test container
     */
    fun getPlugin(): TestPlugin

    /**
     * Gets the DataKache registration instance.
     *
     * @return The DataKacheRegistration instance for this test container
     */
    fun getRegistration(): DataKacheRegistration

    /**
     * Gets the DataKache configuration for this test container.
     *
     * @return The DataKacheConfig instance for this test container
     */
    fun getKacheConfig(): DataKacheConfig
}
