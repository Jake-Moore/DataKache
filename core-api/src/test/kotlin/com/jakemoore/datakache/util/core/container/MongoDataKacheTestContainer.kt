package com.jakemoore.datakache.util.core.container

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.DataKacheContext
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.mode.StorageMode
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.core.serialization.util.SerializationUtil
import com.jakemoore.datakache.util.TestUtil
import com.jakemoore.datakache.util.core.TestDataKacheContext
import com.jakemoore.datakache.util.doc.TestGenericDocCache
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.model.Filters
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import com.mongodb.kotlin.client.coroutine.MongoClient
import org.bson.UuidRepresentation
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.utility.DockerImageName

/**
 * MongoDB-specific implementation of DataKacheTestContainer.
 *
 * Manages MongoDB container lifecycle and provides access to test resources.
 */
@Suppress("UnusedVariable", "unused")
class MongoDataKacheTestContainer(
    private val dbNameShort: String = "TestDatabase"
) : DataKacheTestContainer {

    private lateinit var mongoClient: MongoClient

    private lateinit var config: DataKacheConfig
    private lateinit var context: DataKacheContext
    private var _registration: DataKacheRegistration? = null
    private var _cache: TestGenericDocCache? = null

    override suspend fun beforeSpec() {
        // Validate container is running
        require(container.isRunning) {
            "MongoDB container failed to start"
        }
        require(!container.connectionString.isNullOrBlank()) {
            "MongoDB connection string is empty"
        }

        // Initialize MongoDB client
        val settings = MongoClientSettings.builder().uuidRepresentation(UuidRepresentation.STANDARD)
        settings.applyConnectionString(ConnectionString(container.connectionString))
        mongoClient = MongoClient.create(settings.build())

        // Create context factory and context
        config = DataKacheConfig(
            storageMode = StorageMode.MONGODB,
            mongoURI = container.connectionString
        )
        context = TestDataKacheContext(this)
    }

    override suspend fun beforeEach() {
        // Initialize DataKache
        require(DataKache.onEnable(context)) {
            "Failed to enable DataKache with MongoDB storage mode"
        }

        // Create registration and cache
        TestUtil.createRegistration(databaseName = dbNameShort).also {
            _registration = it
            _cache = TestUtil.createTestGenericDocCache(it)
        }
    }

    private val databaseName: String
        get() = requireNotNull(_registration) {
            "Registration is not initialized. Ensure beforeEach is called."
        }.databaseName

    override suspend fun afterEach() {
        // Shut down registration and cache
        val cache = requireNotNull(this._cache) {
            "Cache is not initialized. Ensure beforeEach is called."
        }
        val reg = requireNotNull(_registration) {
            "Registration is not initialized. Ensure beforeEach is called."
        }

        try {
            // Drop the database so the next test starts with a clean slate
            dropDatabase()
        } finally {
            runCatching { reg.shutdown() }
            // Reset cache and registration
            this._cache = null
            _registration = null

            // Stop DataKache
            require(DataKache.onDisable()) {
                "Failed to disable DataKache after test"
            }
        }
    }

    override suspend fun afterSpec() {
        // Close MongoDB client
        runCatching { mongoClient.close() }
    }

    override val cache: TestGenericDocCache
        get() = requireNotNull(_cache) { "Cache is not initialized. Ensure beforeEach is called." }

    override val registration: DataKacheRegistration
        get() = requireNotNull(_registration) { "Registration is not initialized. Ensure beforeEach is called." }

    override val dataKacheConfig: DataKacheConfig
        get() = config

    override suspend fun <K : Any, D : Doc<K, D>> manualDocumentInsert(cache: DocCache<K, D>, doc: D) {
        val coll = mongoClient.getDatabase(databaseName).getCollection(
            collectionName = cache.cacheName,
            resultClass = cache.docClass,
        )
        coll.insertOne(doc)
    }

    override suspend fun <K : Any, D : Doc<K, D>> manualDocumentUpdate(
        cache: DocCache<K, D>,
        doc: D,
        newVersion: Long,
    ) {
        val coll = mongoClient.getDatabase(databaseName).getCollection(
            collectionName = cache.cacheName,
            resultClass = cache.docClass,
        )
        val keyFieldName = SerializationUtil.getSerialNameForKey(cache)
        val verFieldName = SerializationUtil.getSerialNameForVersion(cache)

        // updateOne setting the version field to the new version
        val update = Updates.set(verFieldName, newVersion)
        val filter = Filters.eq(keyFieldName, cache.keyToString(doc.key))
        coll.updateOne(
            filter = filter,
            update = update,
            options = UpdateOptions().upsert(false),
        )
    }

    override suspend fun <K : Any, D : Doc<K, D>> manualDocumentReplace(cache: DocCache<K, D>, doc: D) {
        val coll = mongoClient.getDatabase(databaseName).getCollection(
            collectionName = cache.cacheName,
            resultClass = cache.docClass,
        )
        val keyFieldName = SerializationUtil.getSerialNameForKey(cache)
        val filter = Filters.eq(keyFieldName, cache.keyToString(doc.key))

        coll.replaceOne(
            filter = filter,
            replacement = doc,
            options = ReplaceOptions().upsert(false)
        )
    }

    override suspend fun <K : Any, D : Doc<K, D>> manualDocumentDelete(cache: DocCache<K, D>, key: K) {
        val coll = mongoClient.getDatabase(databaseName).getCollection(
            collectionName = cache.cacheName,
            resultClass = cache.docClass,
        )
        val keyFieldName = SerializationUtil.getSerialNameForKey(cache)
        val filter = Filters.eq(keyFieldName, cache.keyToString(key))

        coll.deleteOne(filter)
    }

    private suspend fun dropDatabase() {
        runCatching {
            mongoClient.getDatabase(databaseName).drop()
        }.onFailure {
            it.printStackTrace()
        }
    }

    companion object {
        private var _container: MongoDBContainer? = null
        internal fun startContainers() {
            runCatching {
                _container?.stop()
            }

            this._container = MongoDBContainer(DockerImageName.parse("mongo:8.0"))
                .withReuse(false).also { it.start() }
        }

        internal fun stopContainers() {
            runCatching {
                _container?.stop()
            }
            _container = null
        }

        internal val container: MongoDBContainer
            get() = requireNotNull(_container) {
                "MongoDB container is not initialized. Call startContainers() before accessing."
            }
    }
}
