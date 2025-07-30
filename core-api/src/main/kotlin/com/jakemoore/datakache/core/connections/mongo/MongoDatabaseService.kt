package com.jakemoore.datakache.core.connections.mongo

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.core.connections.DatabaseService
import com.jakemoore.datakache.core.serialization.util.SerializationUtil
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.MongoException
import com.mongodb.MongoTimeoutException
import com.mongodb.WriteConcern
import com.mongodb.client.model.Filters
import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoCollection
import com.mongodb.kotlin.client.coroutine.MongoDatabase
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import org.bson.UuidRepresentation
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

internal class MongoDatabaseService : DatabaseService {

    // ------------------------------------------------------------ //
    //                     Mongo Service Properties                 //
    // ------------------------------------------------------------ //
    override var averagePingNanos: Long = -1 // Initial value 0
    internal var mongoClient: MongoClient? = null
    internal var mongoConnected: Boolean = false
    internal var mongoFirstConnect: Boolean = false
    internal var activeListener: UUID = UUID.randomUUID()

    // ------------------------------------------------------------ //
    //                         Service Methods                      //
    // ------------------------------------------------------------ //
    override var running: Boolean = false

    override suspend fun start(): Boolean {
        if (running) {
            this.warn("[MongoDatabaseService#start] MongoDatabaseService is already running!")
            return false
        }

        this.debug("Connecting to MongoDB...")
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
        if (!this.disconnectFromMongoDB()) {
            this.error("Failed to disconnect from MongoDB!")
            return false
        }

        this.running = false
        return true
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
        } catch (t: Throwable) {
            // Catch any exceptions during connection
            this.error(t, "Failed to connect to MongoDB: ${t.message}")
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
            this.info("&aConnection to MongoDB Succeeded! Databases:")
            this.info(databaseNames.joinToString(", ", prefix = "[", postfix = "]"))
        } catch (timeout: MongoTimeoutException) {
            this.error(timeout, "Connection to MongoDB Timed Out!")
            return false
        } catch (t: Throwable) {
            this.error(t, "Failed to connect to MongoDB!")
            return false
        }
        return true
    }

    private fun disconnectFromMongoDB(): Boolean {
        val client = this.mongoClient ?: return true // Already disconnected or never connected

        try {
            this.info("Disconnecting from MongoDB...")
            client.close()
            this.mongoClient = null
            this.mongoConnected = false
            this.databases.clear()
            this.collections.clear()
            this.info("Disconnected from MongoDB successfully.")
            return true
        } catch (t: Throwable) {
            this.error(t, "Failed to disconnect from MongoDB: ${t.message}")
            return false
        }
    }

    // ------------------------------------------------------------ //
    //                         DatabaseService                      //
    // ------------------------------------------------------------ //
    override suspend fun <K : Any, D : Doc<K, D>> save(
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
            var committed = false
            try {
                // Insert the document into the MongoDB collection & commit the transaction
                getMongoCollection(docCache).insertOne(session, doc)
                session.commitTransaction()

                committed = true
                docCache.cacheInternal(doc)
            } finally {
                if (!committed && session.hasActiveTransaction()) {
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

    @Throws(DocumentNotFoundException::class)
    override suspend fun <K : Any, D : Doc<K, D>> update(
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D
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

    override suspend fun <K : Any, D : Doc<K, D>> read(
        docCache: DocCache<K, D>,
        key: K,
    ): D? = withContext(Dispatchers.IO) {
        try {
            val keyFieldName = SerializationUtil.getSerialNameForKey(docCache)
            val doc = getMongoCollection(docCache).find(
                // Find the id field, where the content is the id string
                Filters.eq(keyFieldName, docCache.keyToString(key))
            ).firstOrNull() ?: return@withContext null

            return@withContext doc
        } catch (ex: MongoException) {
            docCache.getLoggerInternal().info(
                ex,
                "MongoException reading Doc (${docCache.getKeyNamespace(key)}) from MongoDB.",
            )
            return@withContext null
        } catch (expected: Exception) {
            docCache.getLoggerInternal().info(
                expected,
                "Exception reading Doc (${docCache.getKeyNamespace(key)}) from MongoDB.",
            )
            return@withContext null
        }
    }

    override suspend fun <K : Any, D : Doc<K, D>> delete(
        docCache: DocCache<K, D>,
        key: K,
    ): Boolean = withContext(Dispatchers.IO) {
        try {
            // Apply a key filter (guarantees uniqueness by mongo's design)
            val keyFieldName = SerializationUtil.getSerialNameForKey(docCache)
            val filter = Filters.eq(keyFieldName, docCache.keyToString(key))

            // Succeeds if mongo reports at least 1 document deleted
            return@withContext getMongoCollection(docCache).deleteMany(filter).deletedCount > 0
        } catch (ex: MongoException) {
            docCache.getLoggerInternal().info(
                ex,
                "MongoException deleting Doc (${docCache.getKeyNamespace(key)}) from MongoDB."
            )
            return@withContext false
        } catch (expected: Exception) {
            docCache.getLoggerInternal().info(
                expected,
                "Exception deleting Doc (${docCache.getKeyNamespace(key)}) from MongoDB."
            )
            return@withContext false
        }
    }

    override fun isDatabaseReadyForWrites(): Boolean {
        return mongoConnected
    }

    // ------------------------------------------------------------ //
    //                   MongoCollection Management                 //
    // ------------------------------------------------------------ //
    private val databases = ConcurrentHashMap<String, MongoDatabase>() // Map<DatabaseName, MongoDatabase>
    private val collections = ConcurrentHashMap<String, MongoCollection<*>>() // Map<Name, MongoCollection>

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
        val database = databases.computeIfAbsent(docCache.databaseName) {
                name: String ->
            client.getDatabase(name)
        }

        // Create or Get the MongoCollection for the specific DocCache
        return database.getCollection(docCache.cacheName, docCache.docClass).also {
            collections[cacheKey] = it
        }
    }
}
