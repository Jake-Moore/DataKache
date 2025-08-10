@file:Suppress("unused")

package com.jakemoore.datakache.util.core

import com.jakemoore.datakache.api.mode.StorageMode
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.util.TestUtil
import com.jakemoore.datakache.util.core.container.DataKacheTestContainer
import com.jakemoore.datakache.util.doc.TestGenericDocCache
import io.kotest.core.spec.style.DescribeSpec
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

    protected lateinit var testContainer: DataKacheTestContainer

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
     * The [TestGenericDocCache] instance for testing.
     */
    val cache: TestGenericDocCache
        get() = testContainer.cache

    /**
     * The [DataKacheRegistration] instance for testing.
     */
    val registration: DataKacheRegistration
        get() = testContainer.registration
}
