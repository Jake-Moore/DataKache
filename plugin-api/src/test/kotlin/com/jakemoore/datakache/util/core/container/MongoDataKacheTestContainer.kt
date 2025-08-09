package com.jakemoore.datakache.util.core.container

import com.jakemoore.datakache.DataKachePlugin
import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.config.DataKachePluginLang
import com.jakemoore.datakache.api.context.DataKachePluginContext
import com.jakemoore.datakache.api.mode.StorageMode
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.util.TestPlugin
import com.jakemoore.datakache.util.TestUtil
import com.jakemoore.datakache.util.doc.TestPlayerDocCache
import org.mockbukkit.mockbukkit.MockBukkit
import org.mockbukkit.mockbukkit.ServerMock
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.utility.DockerImageName
import java.io.File

/**
 * MongoDB-specific implementation of DataKacheTestContainer.
 *
 * Manages MongoDB container lifecycle and provides access to test resources.
 */
@Suppress("UnusedVariable", "unused")
class MongoDataKacheTestContainer(
    private val container: MongoDBContainer,
    private val databaseName: String = "TestDatabase"
) : DataKacheTestContainer {

    private lateinit var config: DataKacheConfig
    private var mockServer: ServerMock? = null
    private var mockPlugin: TestPlugin? = null
    private var registration: DataKacheRegistration? = null
    private var cache: TestPlayerDocCache? = null

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
    }

    override suspend fun beforeEach() {
        // Load the sample plugin.yml in order to register the datakache command properly
        val resource = requireNotNull(this::class.java.classLoader.getResource("plugin.yml"))
        val pluginYml = File(resource.toURI())

        // Start the MockBukkit server
        val server = MockBukkit.mock().also {
            mockServer = it
        }
        val plugin = MockBukkit.loadWith(TestPlugin::class.java, pluginYml).also {
            mockPlugin = it
        }

        val context = DataKachePluginContext(
            plugin = plugin,
            config = config,
            lang = DataKachePluginLang(),
        )

        // Initialize DataKache
        require(DataKachePlugin.enableDataKache(plugin, context)) {
            "Failed to enable DataKache with MongoDB storage mode"
        }

        // Create registration and cache
        TestUtil.createRegistration(databaseName = databaseName).also {
            registration = it
            cache = TestUtil.createTestPlayerDocCache(plugin, it)
        }
    }

    override suspend fun afterEach() {
        // Shut down registration and cache
        val cache = requireNotNull(this.cache) {
            "Cache is not initialized. Ensure beforeEach is called."
        }

        // Clear this collection entirely, preparing for the next test
        cache.clearAllPermanently()
        assert(cache.readSizeFromDatabase().getOrThrow() == 0L) {
            "Cache should be empty after test, but found ${cache.readSizeFromDatabase().getOrThrow()} documents"
        }

        requireNotNull(registration) {
            "Registration is not initialized. Ensure beforeEach is called."
        }.shutdown() // SHOULD shut down the cache too

        // Reset cache and registration
        this.cache = null
        registration = null

        // Shutdown DataKache
        val server = requireNotNull(mockServer) {
            "Server is not initialized. Ensure beforeEach is called."
        }
        val plugin = requireNotNull(mockPlugin) {
            "Plugin is not initialized. Ensure beforeEach is called."
        }
        require(DataKachePlugin.disableDataKache(plugin)) {
            "Failed to disable DataKache after test"
        }

        // Unmock MockBukkit (also shuts down plugin)
        MockBukkit.unmock()
    }

    override suspend fun afterSpec() {
        // Stop container
        container.stop()

        // Double Check MockBukkit
        MockBukkit.unmock()
    }

    override fun getCache(): TestPlayerDocCache = requireNotNull(cache)

    override fun getServer(): ServerMock = requireNotNull(mockServer)

    override fun getPlugin(): TestPlugin = requireNotNull(mockPlugin)

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
