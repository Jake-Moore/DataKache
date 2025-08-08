package com.jakemoore.datakache.core.connections.mongo

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.exception.doc.InvalidDocCopyHelperException
import com.jakemoore.datakache.api.exception.update.DocumentUpdateException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentKeyModificationException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentVersionModificationException
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.exception.update.TransactionRetriesExceededException
import com.jakemoore.datakache.api.exception.update.UpdateFunctionReturnedSameInstanceException
import com.jakemoore.datakache.api.index.DocUniqueIndex
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.core.connections.DatabaseService
import com.jakemoore.datakache.core.connections.changes.ChangeEventHandler
import com.jakemoore.datakache.core.connections.changes.ChangeStreamManager
import com.jakemoore.datakache.core.serialization.util.SerializationUtil
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.MongoException
import com.mongodb.MongoTimeoutException
import com.mongodb.MongoWriteException
import com.mongodb.WriteConcern
import com.mongodb.client.model.Filters
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.Projections
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoCollection
import com.mongodb.kotlin.client.coroutine.MongoDatabase
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import org.bson.Document
import org.bson.UuidRepresentation
import org.bson.types.ObjectId
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

internal class MongoDatabaseService : DatabaseService() {

    // ------------------------------------------------------------ //
    //                     Mongo Service Properties                 //
    // ------------------------------------------------------------ //
    override var averagePingNanos: Long = -1 // Initial value 0
    override val serverPingMap: Cache<String, Long> = CacheBuilder
        .newBuilder()
        .expireAfterWrite(90, TimeUnit.SECONDS)
        .build()

    internal var mongoClient: MongoClient? = null
    internal var mongoConnected: Boolean = false

    // flag representing if mongo has established a successful connection
    //  This is used to determine if players can join the server
    internal var mongoFirstConnect: Boolean = false
    internal var activeListener: UUID = UUID.randomUUID()

    // ------------------------------------------------------------ //
    //                         Service Methods                      //
    // ------------------------------------------------------------ //
    override var running: Boolean = false

    // A strictly internal property representing the current state of the MongoDB connection.
    //  Not to be used except to check if this service intends to keep the MongoDB connection alive.
    internal var keepMongoConnected: Boolean = false
        private set

    override suspend fun start(): Boolean {
        if (running) {
            this.warn("[MongoDatabaseService#start] MongoDatabaseService is already running!")
            return false
        }

        this.debug("Connecting to MongoDB...")
        keepMongoConnected = true
        if (!this.connectToMongoDB()) {
            this.error("Failed to connect to MongoDB! Please check your configuration.")
            return false
        }

        // this.client gets created and set in connectToMongoDB()
        this.running = true
        return true
    }

    override suspend fun shutdown(): Boolean {
        if (!running) {
            this.warn("[MongoDatabaseService#shutdown] MongoDatabaseService is not running!")
            return false
        }

        this.debug("Shutting down MongoDB connection...")

        // First shutdown the parent (which handles UpdateQueueManager)
        val parentShutdownSuccess = super.shutdown()
        if (!parentShutdownSuccess) {
            this.error("Failed to shutdown parent DatabaseService components!")
        }

        // Then shutdown MongoDB-specific components
        keepMongoConnected = false
        if (!this.disconnectFromMongoDB()) {
            this.error("Failed to disconnect from MongoDB!")
            return false
        }

        this.running = false
        return parentShutdownSuccess
    }

    // ------------------------------------------------------------ //
    //                     Mongo Service Properties                 //
    // ------------------------------------------------------------ //
    override val loggerName: String
        get() = "MongoDatabaseService"

    override val permitsDebugStatements: Boolean
        get() = true

    override fun logToConsole(msg: String, level: LoggerService.LogLevel) {
        // Use the logger from DataKache with our prefixed loggerName
        requireNotNull(DataKache.logger).logToConsole("[$loggerName] $msg", level)
    }

    // ------------------------------------------------------------ //
    //                       MongoDB Connection                     //
    // ------------------------------------------------------------ //
    /**
     * @return If the connection to MongoDB was successful.
     */
    private suspend fun connectToMongoDB(): Boolean {
        var client = this.mongoClient
        require(client == null) {
            "MongoClient is already initialized. Please shutdown the service before reconnecting."
        }

        try {
            // Set up the MongoListener
            val listener = MongoListener(this)
            this.activeListener = listener.listenerID

            // Configure Mongo Connection Settings
            val settings = MongoClientSettings.builder()
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .applyToClusterSettings { it.addClusterListener(listener) }
                .applyToServerSettings { it.addServerMonitorListener(listener) }

            // Connect via URI
            settings.applyConnectionString(ConnectionString(MongoConfig.get().uri))
            client = MongoClient.create(settings.build())

            // Validate Connection (ensure the cluster is ready for transactions)
            if (!validateMongoConnection(client)) {
                return false
            }

            // Success!
            return true
        } catch (e: Exception) {
            // Catch any exceptions during connection
            this.error(e, "Failed to connect to MongoDB: ${e.message}")
            return false
        } finally {
            this.mongoClient = client
            this.mongoConnected = client != null
        }
    }

    private suspend fun validateMongoConnection(client: MongoClient): Boolean {
        try {
            this.info("&a&lConnecting to MongoDB via URI... (30 sec. timeout)")
            val databaseNames = client.listDatabaseNames().toList()
            this.info("&aConnection to MongoDB Succeeded! Current namespace Databases:")
            // Only print the databases that are within our current namespace
            //  This means any admin databases or other network databases are not shown
            val viewable = databaseNames
                .filter {
                    it.lowercase().startsWith(DataKache.databaseNamespace.lowercase() + "_")
                }
            this.info(viewable.joinToString(", ", prefix = "[", postfix = "]"))
        } catch (timeout: MongoTimeoutException) {
            this.error(timeout, "Connection to MongoDB Timed Out!")
            return false
        } catch (e: Exception) {
            this.error(e, "Failed to connect to MongoDB!")
            return false
        }
        return true
    }

    private fun disconnectFromMongoDB(): Boolean {
        val client = this.mongoClient ?: return true // Already disconnected or never connected

        try {
            this.debug("Disconnecting from MongoDB...")
            client.close()
            this.mongoClient = null
            this.mongoConnected = false
            this.databases.clear()
            this.collections.clear()
            this.info("&aDisconnected from MongoDB successfully.")
            return true
        } catch (e: Exception) {
            this.error(e, "Failed to disconnect from MongoDB: ${e.message}")
            return false
        }
    }

    // ------------------------------------------------------------ //
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //
    @Throws(DuplicateDocumentKeyException::class, DuplicateUniqueIndexException::class)
    override suspend fun <K : Any, D : Doc<K, D>> insertInternal(
        docCache: DocCache<K, D>,
        doc: D,
    ) = withContext(Dispatchers.IO) {
        // MongoDB uses "_id" as the ID field, ensure the id property is serializing correctly
        val tempIdName = SerializationUtil.getSerialNameForKey(docCache)
        if (tempIdName != "_id") {
            throw IllegalStateException(
                "MongoDB uses '_id' as the ID field! " +
                    "Ensure your Doc.id property is annotated with @SerialName(\"_id\"). " +
                    "Your current ID field is set to serialize as: '$tempIdName'"
            )
        }

        val client = requireNotNull(mongoClient) {
            "MongoClient is not initialized! Could not save Doc to MongoDB!"
        }

        // Start a transaction session to ensure atomicity while we save the document
        client.startSession().use { session ->
            session.startTransaction()
            var sessionClosed = false
            try {
                // Insert the document into the MongoDB collection & commit the transaction
                getMongoCollection(docCache).insertOne(session, doc)

                session.commitTransaction()
                sessionClosed = true

                docCache.cacheInternal(doc)
            } catch (e: MongoWriteException) {
                if (!sessionClosed && session.hasActiveTransaction()) {
                    session.abortTransaction()
                    sessionClosed = true
                }

                // Transform certain MongoWrite exceptions into standard DataKache exceptions
                if (e.error.code == DUPLICATE_KEY_VIOLATION_CODE) {
                    val errorMessage = e.message ?: ""
                    when {
                        errorMessage.contains("index: _id") -> {
                            // Primary key violation (duplicate _id)
                            throw DuplicateDocumentKeyException(
                                docCache = docCache,
                                docCache.keyToString(doc.key),
                                fullMessage = errorMessage,
                                operation = "insert",
                            )
                        }
                        errorMessage.contains("index:") -> {
                            // Unique index violation (duplicate value in a unique index)
                            throw DuplicateUniqueIndexException(
                                docCache = docCache,
                                fullMessage = errorMessage,
                                operation = "insert",
                            )
                        }
                    }
                }

                // Any Other WriteException -> Promote to caller (no additional logging needed)
                throw e
            } finally {
                if (!sessionClosed && session.hasActiveTransaction()) {
                    session.abortTransaction()
                    docCache.getLoggerInternal().severe(
                        "Failed to commit transaction (save) for Doc " +
                            "(${docCache.getKeyNamespace(doc.key)}) in MongoDB. " +
                            "Transaction has been aborted."
                    )
                }
            }
        }
    }

    @Throws(
        DocumentNotFoundException::class, DuplicateUniqueIndexException::class,
        TransactionRetriesExceededException::class, DocumentUpdateException::class,
        InvalidDocCopyHelperException::class, UpdateFunctionReturnedSameInstanceException::class,
        IllegalDocumentKeyModificationException::class, IllegalDocumentVersionModificationException::class,
        RejectUpdateException::class,
    )
    override suspend fun <K : Any, D : Doc<K, D>> updateInternal(
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D,
    ): D = withContext(Dispatchers.IO) {
        val client = requireNotNull(mongoClient) {
            "MongoClient is not initialized! Could not update Doc in MongoDB!"
        }
        // Using WriteConcern.MAJORITY to ensure the write is acknowledged by the majority of nodes
        val collection = getMongoCollection(docCache).withWriteConcern(WriteConcern.MAJORITY)
        return@withContext MongoTransactions.update(
            client,
            collection,
            docCache,
            doc,
            updateFunction,
        )
    }

    override suspend fun <K : Any, D : Doc<K, D>> readInternal(
        docCache: DocCache<K, D>,
        key: K,
    ): D? = withContext(Dispatchers.IO) {
        try {
            val keyFieldName = SerializationUtil.getSerialNameForKey(docCache)
            return@withContext getMongoCollection(docCache).find(
                // Find the id field, where the content is the id string
                Filters.eq(keyFieldName, docCache.keyToString(key))
            ).firstOrNull()
        } catch (me: MongoException) {
            docCache.getLoggerInternal().severe(
                me,
                "MongoException reading Doc (${docCache.getKeyNamespace(key)}) from MongoDB.",
            )
            throw me
        } catch (e: Exception) {
            docCache.getLoggerInternal().severe(
                e,
                "Exception reading Doc (${docCache.getKeyNamespace(key)}) from MongoDB.",
            )
            throw e
        }
    }

    override suspend fun <K : Any, D : Doc<K, D>> deleteInternal(
        docCache: DocCache<K, D>,
        key: K,
    ): Boolean = withContext(Dispatchers.IO) {
        try {
            // Apply a key filter (guarantees uniqueness by mongo's design)
            val keyFieldName = SerializationUtil.getSerialNameForKey(docCache)
            val filter = Filters.eq(keyFieldName, docCache.keyToString(key))

            // Succeeds if mongo reports at least 1 document deleted
            return@withContext getMongoCollection(docCache).deleteMany(filter).deletedCount > 0
        } catch (me: MongoException) {
            docCache.getLoggerInternal().severe(
                me,
                "MongoException deleting Doc (${docCache.getKeyNamespace(key)}) from MongoDB."
            )
            throw me
        } catch (e: Exception) {
            docCache.getLoggerInternal().severe(
                e,
                "Exception deleting Doc (${docCache.getKeyNamespace(key)}) from MongoDB."
            )
            throw e
        }
    }

    override suspend fun <K : Any, D : Doc<K, D>> readAllInternal(
        docCache: DocCache<K, D>,
    ): Flow<D> = withContext(Dispatchers.IO) {
        getMongoCollection(docCache).find().map { doc: D ->
            // Ensure doc is initialized with its backing cache
            doc.initializeInternal(docCache)
            // We read the document again, might as well cache it for consistency
            docCache.cacheInternal(doc, log = false)
            doc
        }
    }

    override suspend fun <K : Any, D : Doc<K, D>> sizeInternal(
        docCache: DocCache<K, D>,
    ): Long = withContext(Dispatchers.IO) {
        getMongoCollection(docCache).countDocuments()
    }

    override suspend fun <K : Any, D : Doc<K, D>> hasKeyInternal(
        docCache: DocCache<K, D>,
        key: K,
    ): Boolean = withContext(Dispatchers.IO) {
        try {
            val keyFieldName = SerializationUtil.getSerialNameForKey(docCache)
            val filter = Filters.eq(keyFieldName, docCache.keyToString(key))

            // Instead of counting with our filter, just try to find 1 matching document
            //  This is more efficient and avoids the overhead of counting/scanning every document
            return@withContext getRawCollection(docCache)
                .find(filter)
                .limit(1)
                .firstOrNull() != null
        } catch (me: MongoException) {
            docCache.getLoggerInternal().severe(
                throwable = me,
                msg = "MongoException on DocCache.hasKey (${docCache.getKeyNamespace(key)})"
            )
            throw me
        } catch (e: Exception) {
            docCache.getLoggerInternal().severe(
                throwable = e,
                msg = "Exception on DocCache.hasKey (${docCache.getKeyNamespace(key)})"
            )
            throw e
        }
    }

    override suspend fun <K : Any, D : Doc<K, D>> clearInternal(
        docCache: DocCache<K, D>,
    ): Long = withContext(Dispatchers.IO) {
        try {
            // Delete with NO FILTER!
            return@withContext getMongoCollection(docCache)
                .deleteMany(Filters.empty())
                .deletedCount
        } catch (me: MongoException) {
            docCache.getLoggerInternal().info(
                throwable = me,
                msg = "MongoException on DocCache.clear (${docCache.cacheName})"
            )
            throw me
        } catch (e: Exception) {
            docCache.getLoggerInternal().info(
                throwable = e,
                msg = "Exception on DocCache.clear (${docCache.cacheName})"
            )
            throw e
        }
    }

    override suspend fun <K : Any, D : Doc<K, D>> readKeysInternal(
        docCache: DocCache<K, D>,
    ): Flow<K> = withContext(Dispatchers.IO) {
        try {
            val keyFieldName = SerializationUtil.getSerialNameForKey(docCache)

            getRawCollection(docCache)
                .find()
                .projection(Projections.include(keyFieldName))
                .mapNotNull { getKeyByRawDocument(it, keyFieldName, docCache) }
        } catch (me: MongoException) {
            docCache.getLoggerInternal().info(
                throwable = me,
                msg = "MongoException on DocCache.clear (${docCache.cacheName})"
            )
            throw me
        } catch (e: Exception) {
            docCache.getLoggerInternal().info(
                throwable = e,
                msg = "Exception on DocCache.clear (${docCache.cacheName})"
            )
            throw e
        }
    }

    @Throws(DocumentNotFoundException::class)
    override suspend fun <K : Any, D : Doc<K, D>> replaceInternal(
        docCache: DocCache<K, D>,
        key: K,
        update: D,
    ) = withContext(Dispatchers.IO) {
        val k1 = docCache.keyToString(key)
        val k2 = docCache.keyToString(update.key)
        require(k1 == k2) {
            "Key mismatch! Cannot replace document with key '$k1' using document with key '$k2'. " +
                "Ensure the keys match before replacing."
        }

        try {
            val keyFieldName = SerializationUtil.getSerialNameForKey(docCache)
            val filter = Filters.eq(keyFieldName, k1)

            // upsert=false means it will not insert a new document if the key does not exist
            val options = ReplaceOptions().upsert(false)
            val result = getMongoCollection(docCache)
                .replaceOne(filter, update, options)

            // Fail State - No Document Replaced
            if (result.matchedCount == 0L) {
                val keyString = docCache.keyToString(key)
                throw DocumentNotFoundException(
                    keyString = keyString,
                    docCache = docCache,
                    operation = "replace",
                )
            }
        } catch (me: MongoException) {
            docCache.getLoggerInternal().info(
                throwable = me,
                msg = "MongoException on DocCache.replace (${docCache.cacheName})"
            )
            throw me
        } catch (e: Exception) {
            docCache.getLoggerInternal().info(
                throwable = e,
                msg = "Exception on DocCache.replace (${docCache.cacheName})"
            )
            throw e
        }
    }

    // ------------------------------------------------------------ //
    //                         Unique Indexes                       //
    // ------------------------------------------------------------ //
    override suspend fun <K : Any, D : Doc<K, D>, T> registerUniqueIndexInternal(
        docCache: DocCache<K, D>,
        index: DocUniqueIndex<K, D, T>
    ) = withContext(Dispatchers.IO) {
        try {
            // Create the new index (as unique)
            getMongoCollection(docCache).createIndex(
                Document(index.fieldName, 1),
                IndexOptions().unique(true)
            )
            return@withContext
        } catch (me: MongoException) {
            docCache.getLoggerInternal().info(
                throwable = me,
                msg = "MongoException on registerUniqueIndex (${docCache.cacheName})"
            )
            throw me
        } catch (e: Exception) {
            docCache.getLoggerInternal().info(
                throwable = e,
                msg = "Exception on registerUniqueIndex (${docCache.cacheName})"
            )
            throw e
        }
    }

    override suspend fun <K : Any, D : Doc<K, D>, T> readByUniqueIndexInternal(
        docCache: DocCache<K, D>,
        index: DocUniqueIndex<K, D, T>,
        value: T
    ): D? = withContext(Dispatchers.IO) {
        try {
            val indexFilter = Filters.eq(index.fieldName, value)
            val doc: D = getMongoCollection(docCache)
                .find(indexFilter)
                .firstOrNull() ?: return@withContext null

            val v2 = index.extractValue(doc)
            if (!index.equals(value, v2)) {
                warn(
                    "Index mismatch! " +
                        "The value '$value' does not match the index value '$v2' " +
                        "for document with key '${doc.key}' in DocCache '${docCache.cacheName}'."
                )
                return@withContext null
            }
            return@withContext doc
        } catch (me: MongoException) {
            docCache.getLoggerInternal().info(
                throwable = me,
                msg = "MongoException on readByUniqueIndex (${docCache.cacheName})"
            )
            throw me
        } catch (e: Exception) {
            docCache.getLoggerInternal().info(
                throwable = e,
                msg = "Exception on readByUniqueIndex (${docCache.cacheName})"
            )
            throw e
        }
    }

    // ------------------------------------------------------------ //
    //                            MISC API                          //
    // ------------------------------------------------------------ //
    override fun isDatabaseReadyForWrites(): Boolean {
        // Must have a successful first connection from the Listener
        //  AND must be currently connected to MongoDB
        return mongoFirstConnect && mongoConnected
    }

    // ------------------------------------------------------------ //
    //                   MongoCollection Management                 //
    // ------------------------------------------------------------ //
    private val databases = ConcurrentHashMap<String, MongoDatabase>() // Map<DatabaseName, MongoDatabase>
    private val collections = ConcurrentHashMap<String, MongoCollection<*>>() // Map<Name, MongoCollection>
    private val rawCollections = ConcurrentHashMap<String, MongoCollection<Document>>() // Map<Name, MongoCollection>

    @Suppress("UNCHECKED_CAST")
    private fun <K : Any, D : Doc<K, D>> getMongoCollection(docCache: DocCache<K, D>): MongoCollection<D> {
        val client = requireNotNull(this.mongoClient) {
            "MongoClient is not initialized! Could not fetch MongoCollection!"
        }
        // Unique key that identifies the cache in this specific database
        val cacheKey = docCache.databaseName + "." + docCache.cacheName

        // Return existing cached collection if available
        collections[cacheKey]?.let {
            return it as MongoCollection<D>
        }

        // Create or Get the current Database from MongoDB
        val database = databases.computeIfAbsent(docCache.databaseName) { name: String ->
            client.getDatabase(name)
        }

        // Create or Get the MongoCollection for the specific DocCache
        return database
            .getCollection(docCache.cacheName, docCache.docClass)
            .also { collections[cacheKey] = it }
    }

    @Suppress("UNCHECKED_CAST")
    private fun <K : Any, D : Doc<K, D>> getRawCollection(docCache: DocCache<K, D>): MongoCollection<Document> {
        val client = requireNotNull(this.mongoClient) {
            "MongoClient is not initialized! Could not fetch raw MongoCollection!"
        }
        // Unique key that identifies the cache in this specific database
        val rawKey = docCache.databaseName + "." + docCache.cacheName

        // Return existing cached collection if available
        rawCollections[rawKey]?.let { return it }

        // Create or Get the current Database from MongoDB
        val database = databases.computeIfAbsent(docCache.databaseName) { name: String ->
            client.getDatabase(name)
        }

        // Create or Get the MongoCollection for the specific DocCache
        return database
            .getCollection<Document>(docCache.cacheName)
            .also { rawCollections[rawKey] = it }
    }

    @Suppress("CanConvertToMultiDollarString")
    override suspend fun getCurrentOperationTime(): Any? {
        return try {
            val client = requireNotNull(this.mongoClient) {
                "MongoClient is not initialized! Could not get current operation time!"
            }

            // Get cluster time by running a simple operation
            val adminDatabase = client.getDatabase("admin")
            val result = adminDatabase.runCommand(Document("hello", 1))

            // Extract cluster time from the result
            val clusterTimeDoc = result["\$clusterTime"] as? Document
            clusterTimeDoc?.get("clusterTime") as? org.bson.BsonTimestamp
        } catch (e: Exception) {
            this.warn("Failed to get current operation time: ${e.message}")
            null
        }
    }

    override suspend fun <K : Any, D : Doc<K, D>> createChangeStreamManager(
        docCache: DocCache<K, D>,
        eventHandler: ChangeEventHandler<K, D>
    ): ChangeStreamManager<K, D> {
        val collection = getMongoCollection(docCache)
        return MongoChangeStreamManager(
            collection = collection,
            eventHandler = eventHandler,
            logger = this
        )
    }

    // ------------------------------------------------------------ //
    //                         Helper Methods                       //
    // ------------------------------------------------------------ //
    private fun <D : Doc<K, D>, K : Any> getKeyByRawDocument(
        document: Document,
        keyFieldName: String,
        docCache: DocCache<K, D>
    ): K? {
        return when (val field = document.get(keyFieldName)) {
            is String -> docCache.keyFromString(field)
            is ObjectId -> docCache.keyFromString(field.toHexString())
            else -> {
                docCache.getLoggerInternal().warning(
                    "[#readKeys] Unexpected key type for DocCache '${docCache.cacheName}': " +
                        "Expected String or ObjectId, got ${field?.javaClass?.simpleName ?: "null"}"
                )
                null
            }
        }
    }

    companion object {
        internal const val DUPLICATE_KEY_VIOLATION_CODE = 11000
    }
}
