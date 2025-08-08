package com.jakemoore.datakache.util.core.container

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.DataKacheContext
import com.jakemoore.datakache.api.mode.StorageMode
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.util.TestUtil
import com.jakemoore.datakache.util.core.TestDataKacheContext
import com.jakemoore.datakache.util.doc.TestGenericDocCache
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.utility.DockerImageName

/**
 * MongoDB-specific implementation of DataKacheTestContainer.
 *
 * Manages MongoDB container lifecycle and provides access to test resources.
 */
class MongoDataKacheTestContainer(
    private val container: MongoDBContainer,
    private val databaseName: String = "TestDatabase"
) : DataKacheTestContainer {

    private lateinit var config: DataKacheConfig
    private lateinit var context: DataKacheContext
    private var registration: DataKacheRegistration? = null
    private var cache: TestGenericDocCache? = null

    override suspend fun beforeSpec() {
        // Start the MongoDB container
        container.start()

        // Validate container is running
        require(container.isRunning) {
            "MongoDB container failed to start"
        }
        require(!container.connectionString.isNullOrBlank()) {
            "MongoDB connection string is empty"
        }

        // Create context factory and context
        config = DataKacheConfig(
            storageMode = StorageMode.MONGODB,
            mongoURI = container.connectionString
        )
        context = TestDataKacheContext(this)

        // Initialize DataKache
        val enabled = DataKache.onEnable(context)
        if (!enabled) {
            throw IllegalStateException("Failed to enable DataKache for test container")
        }
    }

    override suspend fun beforeEach() {
        // Create registration and cache
        TestUtil.createRegistration(databaseName = databaseName).also {
            registration = it
            cache = TestUtil.createTestGenericDocCache(it)
        }
    }

    override suspend fun afterEach() {
        // Shut down registration and cache
        val cache = requireNotNull(this.cache) {
            "Cache is not initialized. Ensure beforeEach is called."
        }

        // Clear this collection entirely, preparing for the next test
        DataKache.storageMode.databaseService.clear(cache)
        assert(cache.readSizeFromDatabase().getOrThrow() == 0L) {
            "Cache should be empty after test, but found ${cache.readSizeFromDatabase().getOrThrow()} documents"
        }

        cache.shutdown()
        registration?.shutdown()

        // Reset cache and registration
        this.cache = null
        registration = null
    }

    override suspend fun afterSpec() {
        // Stop DataKache
        val disabled = DataKache.onDisable()
        if (!disabled) {
            System.err.println("Warning: DataKache was already disabled or failed to disable properly")
        }

        // Stop container
        container.stop()
    }

    override fun getCache(): TestGenericDocCache = requireNotNull(cache)

    override fun getRegistration(): DataKacheRegistration = requireNotNull(registration)

    override fun getKacheConfig(): DataKacheConfig = config

    companion object {
        /**
         * Creates a new MongoDataKacheTestContainer with default MongoDB 8.0 image.
         *
         * @param databaseName The name of the test database
         * @return A new MongoDataKacheTestContainer instance
         */
        fun create(databaseName: String = "TestDatabase"): MongoDataKacheTestContainer {
            // Use only one docker container instance for all tests
            val container = MongoDBContainer(DockerImageName.parse("mongo:8.0"))
                .withReuse(false)
            return MongoDataKacheTestContainer(container, databaseName)
        }
    }
}
