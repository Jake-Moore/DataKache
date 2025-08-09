@file:Suppress("unused")

package com.jakemoore.datakache.util.core

import com.jakemoore.datakache.api.mode.StorageMode
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.util.TestUtil
import com.jakemoore.datakache.util.core.container.DataKacheTestContainer
import com.jakemoore.datakache.util.doc.TestGenericDocCache
import io.kotest.core.spec.style.DescribeSpec
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json

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
     * @return The TestGenericDocCache instance for this test
     */
    protected fun getCache(): TestGenericDocCache = testContainer.getCache()

    /**
     * Gets the DataKache registration instance.
     *
     * @return The DataKacheRegistration instance for this test
     */
    protected fun getRegistration(): DataKacheRegistration = testContainer.getRegistration()
}
