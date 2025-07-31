package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.api.result.RejectableResult
import com.jakemoore.datakache.api.result.handler.DeleteResultHandler
import com.jakemoore.datakache.api.result.handler.ReadResultHandler
import com.jakemoore.datakache.api.result.handler.RejectableUpdateResultHandler
import com.jakemoore.datakache.api.result.handler.UpdateResultHandler
import com.jakemoore.datakache.core.connections.changes.ChangeEventHandler
import com.jakemoore.datakache.core.connections.changes.ChangeOperationType
import com.jakemoore.datakache.core.connections.changes.ChangeStreamManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.ApiStatus
import java.util.concurrent.ConcurrentHashMap

abstract class DocCacheImpl<K : Any, D : Doc<K, D>>(
    override val cacheName: String,
    override val registration: DataKacheRegistration,
    override val docClass: Class<D>,
    /**
     * @param String - the cache name
     */
    private val loggerInstantiator: (String) -> LoggerService,
) : DocCache<K, D> {

    override val databaseName: String
        get() = registration.databaseName

    // ------------------------------------------------------------ //
    //                         Service Methods                      //
    // ------------------------------------------------------------ //
    var running: Boolean = false
    private var changeStreamManager: ChangeStreamManager<K, D>? = null

    /**
     * Internal method, which should only be called by [DataKacheRegistration.registerDocCache]
     *
     * @return If this call started the service (false if already running)
     */
    internal suspend fun start(): Boolean {
        if (running) return false

        try {
            // CRITICAL FIX: Capture operation time BEFORE loading documents to prevent timing gaps
            val operationTime = DataKache.storageMode.databaseService.getCurrentOperationTime()
            this.getLoggerInternal().debug(
                "Captured operation time before loading: $operationTime for cache: $cacheName"
            )

            // Preload all Documents into Cache
            loadAllIntoCache()
            this.getLoggerInternal().info("Loaded all documents (${cacheMap.size}x) into cache: $cacheName")

            // Listen for DB Updates that should be streamed down
            // Pass the captured operation time to prevent timing gaps
            startChangeStreamListener(operationTime)

            running = true
            this.getLoggerInternal().info("Successfully started cache: $cacheName")
            return true
        } catch (e: Exception) {
            this.getLoggerInternal().error(e, "Failed to start cache: $cacheName")

            // Cleanup on failure
            try {
                cleanupOnStartupFailure()
            } catch (cleanupException: Exception) {
                this.getLoggerInternal().error(
                    cleanupException,
                    "Error during startup failure cleanup for cache: $cacheName"
                )
            }

            throw e
        }
    }

    /**
     * Cleans up resources if startup fails.
     */
    private suspend fun cleanupOnStartupFailure() {
        this.getLoggerInternal().debug("Cleaning up resources after startup failure for cache: $cacheName")

        // Stop any partially started change stream
        changeStreamManager?.stop()
        changeStreamManager = null

        // Clear any loaded documents
        cacheMap.clear()

        // Reset running state
        running = false
    }

    /**
     * Internal method, which should only be called from [DataKacheRegistration.shutdown]
     *
     * @return If this call shutdown the service (false if already stopped)
     */
    internal suspend fun shutdown(): Boolean {
        if (!running) return false

        // Stop the change stream manager and properly await completion
        try {
            changeStreamManager?.stop()
            changeStreamManager = null
        } catch (e: Exception) {
            getLoggerInternal().error(e, "Error during change stream shutdown: $cacheName")
            // Continue with shutdown despite change stream errors
        }

        // Shutdown the super cache
        val superShutdownSuccess = shutdownSuper()

        // Mark as not running
        running = false

        // Unregister the cache
        try {
            registration.onDocCacheShutdown(this)
        } catch (e: Exception) {
            getLoggerInternal().error(e, "Error during cache unregistration: $cacheName")
            return false
        }

        getLoggerInternal().info("Cache shutdown completed: $cacheName")
        return superShutdownSuccess
    }
    protected abstract suspend fun shutdownSuper(): Boolean

    // ------------------------------------------------------------ //
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //
    private val cacheMap: MutableMap<K, D> = ConcurrentHashMap()

    override fun read(key: K): OptionalResult<D> {
        return ReadResultHandler.wrap {
            return@wrap cacheMap[key]
        }
    }

    @Throws(DocumentNotFoundException::class)
    override suspend fun update(key: K, updateFunction: (D) -> D): DefiniteResult<D> {
        return UpdateResultHandler.wrap {
            return@wrap updateInternal(key, updateFunction)
        }
    }

    @Throws(DocumentNotFoundException::class)
    override suspend fun updateRejectable(key: K, updateFunction: (D) -> D): RejectableResult<D> {
        return RejectableUpdateResultHandler.wrap {
            return@wrap updateInternal(key, updateFunction)
        }
    }

    @Throws(DocumentNotFoundException::class)
    private suspend fun updateInternal(key: K, updateFunction: (D) -> D): D {
        val doc = cacheMap[key] ?: throw DocumentNotFoundException(key, this)
        return DataKache.storageMode.databaseService.update(this, doc, updateFunction)
    }

    override suspend fun delete(key: K): DefiniteResult<Boolean> {
        return DeleteResultHandler.wrap {
            val found = cacheMap.remove(key) != null
            DataKache.storageMode.databaseService.delete(this, key)
            cacheMap.remove(key)

            // This method boolean is whether the document was found in the cache
            //   false is okay, just indicates that the document was not cached
            return@wrap found
        }
    }

    override fun isCached(key: K): Boolean {
        return cacheMap.containsKey(key)
    }

    // ------------------------------------------------------------ //
    //                     Internal Cache Methods                   //
    // ------------------------------------------------------------ //
    @ApiStatus.Internal
    override fun cacheInternal(doc: D, log: Boolean) {
        cacheMap[doc.key] = doc
        if (log) {
            getLoggerInternal().debug("Cached document: ${doc.key}")
        }
        doc.initializeInternal(this)
    }

    @ApiStatus.Internal
    override fun uncacheInternal(doc: D): Boolean {
        return cacheMap.remove(doc.key) != null
    }

    @ApiStatus.Internal
    override fun uncacheInternal(key: K): Boolean {
        return cacheMap.remove(key) != null
    }

    /**
     * @return The same [doc] for chaining.
     */
    @ApiStatus.Internal
    suspend fun saveDatabaseInternal(doc: D): D {
        // Save the document to the database
        DataKache.storageMode.databaseService.save(this, doc)
        // Cache the document in memory
        this.cacheInternal(doc)
        return doc
    }

    // ------------------------------------------------------------ //
    //                      Cache Logger Service                    //
    // ------------------------------------------------------------ //
    private var _loggerService: LoggerService? = null

    @ApiStatus.Internal
    override fun getLoggerInternal(): LoggerService {
        val service = _loggerService
        if (service != null) {
            return service
        }
        return loggerInstantiator(this.cacheName).also {
            this._loggerService = it
        }
    }

    // ------------------------------------------------------------ //
    //                        MongoDB Streams                       //
    // ------------------------------------------------------------ //
    private suspend fun loadAllIntoCache() = withContext(Dispatchers.IO) {
        val documents = DataKache.storageMode.databaseService.readAll(this@DocCacheImpl)
        documents.collect { doc ->
            cacheInternal(doc, log = false)
        }
    }

    private suspend fun startChangeStreamListener(operationTime: Any?) {
        // Create the change stream manager through the database service
        DataKache.storageMode.databaseService.createChangeStreamManager(
            this@DocCacheImpl,
            createChangeEventHandler()
        ).also {
            changeStreamManager = it

            // Start the change stream with pre-captured operation time to prevent timing gaps
            it.start(operationTime)

            getLoggerInternal().info(
                "Started change stream listener for cache: $cacheName with operation time: $operationTime"
            )
        }
    }

    private fun createChangeEventHandler(): ChangeEventHandler<K, D> {
        return object : ChangeEventHandler<K, D> {
            override suspend fun onDocumentChanged(doc: D, operationType: ChangeOperationType) {
                when (operationType) {
                    ChangeOperationType.INSERT, ChangeOperationType.REPLACE, ChangeOperationType.UPDATE -> {
                        cacheInternal(doc, log = false)
                        getLoggerInternal().debug("Cached Document From ${operationType.name}: ${doc.key}")
                    }
                    else -> {
                        getLoggerInternal().warn(
                            "Unhandled operation type: $operationType " +
                                "for document: ${doc.key}"
                        )
                    }
                }
            }

            override suspend fun onDocumentDeleted(key: K) {
                val removed = uncacheInternal(key)
                if (removed) {
                    getLoggerInternal().debug("Document deleted: $key")
                }
            }

            override suspend fun onConnected() {
                getLoggerInternal().info("Change stream connected for cache: $cacheName")
            }

            override suspend fun onDisconnected() {
                getLoggerInternal().warn("Change stream disconnected for cache: $cacheName")
            }
        }
    }
}
