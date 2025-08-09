package com.jakemoore.datakache.util.core.container

import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.util.doc.TestGenericDocCache

/**
 * Abstract container for managing DataKache test infrastructure.
 *
 * This interface provides a database-agnostic way to manage test containers,
 * including setup, teardown, and access to test resources.
 */
@Suppress("unused")
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
     * The [TestGenericDocCache] instance for testing.
     */
    val cache: TestGenericDocCache

    /**
     * The [DataKacheRegistration] instance for testing.
     */
    val registration: DataKacheRegistration

    /**
     * The [DataKacheConfig] instance for testing.
     */
    val kacheConfig: DataKacheConfig
}
