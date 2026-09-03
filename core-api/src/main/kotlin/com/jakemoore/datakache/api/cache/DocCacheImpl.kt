package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.changes.ChangeDocumentType
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.exception.data.Operation
import com.jakemoore.datakache.api.exception.doc.InvalidDocCopyHelperException
import com.jakemoore.datakache.api.exception.update.DocumentUpdateException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentKeyModificationException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentVersionModificationException
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.exception.update.TransactionRetriesExceededException
import com.jakemoore.datakache.api.exception.update.UpdateFunctionReturnedSameInstanceException
import com.jakemoore.datakache.api.index.DocUniqueIndex
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiver
import com.jakemoore.datakache.api.ordering.OperationTime
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.api.result.handler.ReadResultHandler
import com.jakemoore.datakache.api.result.handler.ReadUniqueIndexResultHandler
import com.jakemoore.datakache.api.result.handler.database.DbClearResultHandler
import com.jakemoore.datakache.api.result.handler.database.DbHasKeyResultHandler
import com.jakemoore.datakache.api.result.handler.database.DbReadAllResultHandler
import com.jakemoore.datakache.api.result.handler.database.DbReadKeysResultHandler
import com.jakemoore.datakache.api.result.handler.database.DbReadResultHandler
import com.jakemoore.datakache.api.result.handler.database.DbReadUniqueIndexResultHandler
import com.jakemoore.datakache.api.result.handler.database.DbRegisterUniqueIndexResultHandler
import com.jakemoore.datakache.api.result.handler.database.DbSizeResultHandler
import com.jakemoore.datakache.core.connections.changes.ChangeEventHandler
import com.jakemoore.datakache.core.connections.changes.ChangeStreamManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.ApiStatus
import java.util.Collections
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
            // Ensure the backing Collection is created on the majority of nodes, ready for us after this call
            DataKache.storageMode.databaseService.ensureCollectionExists(this)

            // Capture operation time BEFORE loading documents to prevent timing gaps
            val operationTime = DataKache.storageMode.databaseService.getCurrentOperationTime()
            this.getLoggerInternal().debug(
                "Captured operation time before loading: $operationTime for cache: $cacheName",
            )

            // Preload all Documents into Cache
            loadAllIntoCache(operationTime)
            this.getLoggerInternal().debug("Loaded all documents (${cacheMap.size}x) into cache: $cacheName")

            // Listen for DB Updates that should be streamed down
            // Pass the captured operation time to prevent timing gaps
            startChangeStreamListener(operationTime)

            running = true
            this.getLoggerInternal().debug("Successfully started cache: $cacheName")
            return true
        } catch (e: Exception) {
            this.getLoggerInternal().error(e, "Failed to start cache: $cacheName")

            // Cleanup on failure
            try {
                cleanupOnStartupFailure()
            } catch (cleanupException: Exception) {
                this.getLoggerInternal().error(
                    cleanupException,
                    "Error during startup failure cleanup for cache: $cacheName",
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
        val superShutdownSuccess = shutdownDocCache()

        // Mark as not running
        cacheMap.clear()
        running = false

        // Unregister the cache
        try {
            registration.onDocCacheShutdown(this)
        } catch (e: Exception) {
            getLoggerInternal().error(e, "Error during cache unregistration: $cacheName")
            return false
        }

        getLoggerInternal().debug("Cache shutdown completed: $cacheName")
        return superShutdownSuccess
    }

    // nothing, intended for super class overrides
    protected open suspend fun shutdownDocCache(): Boolean = true

    // ------------------------------------------------------------ //
    //                          API Methods                         //
    // ------------------------------------------------------------ //
    override fun getStatus(key: K, version: Long): Doc.Status {
        val cachedDoc: D? = cacheMap[key]
        return if (cachedDoc == null) {
            Doc.Status.DELETED
        } else if (cachedDoc.version == version) {
            Doc.Status.FRESH
        } else {
            Doc.Status.STALE
        }
    }

    // ------------------------------------------------------------ //
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //
    protected val cacheMap: MutableMap<K, D> = ConcurrentHashMap()

    override fun read(key: K): OptionalResult<D> {
        return ReadResultHandler.wrap {
            return@wrap cacheMap[key]
        }
    }

    @Throws(
        DocumentNotFoundException::class, DuplicateUniqueIndexException::class,
        TransactionRetriesExceededException::class, DocumentUpdateException::class,
        InvalidDocCopyHelperException::class, UpdateFunctionReturnedSameInstanceException::class,
        IllegalDocumentKeyModificationException::class, IllegalDocumentVersionModificationException::class,
        RejectUpdateException::class,
    )
    protected suspend fun updateInternal(key: K, updateFunction: (D) -> D, bypassValidation: Boolean): D {
        // Read from the database because having a false negative cache hit is worse than waiting for the database read.
        val doc: D =
            this.readFromDatabase(key).getOrNull() ?: run {
                // METRICS
                DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseUpdateDocNotFoundFail)

                val keyString = this.keyToString(key)
                throw DocumentNotFoundException(
                    keyString = keyString,
                    docCache = this,
                    operation = Operation.UPDATE,
                )
            }
        return DataKache.storageMode.databaseService
            .update(this, doc, updateFunction, bypassValidation)
    }

    override fun readAll(): Collection<D> = Collections.unmodifiableCollection(cacheMap.values)

    override fun getKeys(): Set<K> = Collections.unmodifiableSet(cacheMap.keys)

    override fun isCached(key: K): Boolean = cacheMap.containsKey(key)

    override fun getCacheSize(): Int = cacheMap.size

    override suspend fun clearDocsFromDatabasePermanently(): DefiniteResult<Long> {
        check(config.enableMassDestructiveOps) {
            "Cannot clear documents from database permanently when " +
                "enableMassDestructiveOps is set to false in cache config."
        }

        return DbClearResultHandler.wrap {
            // Clear the database collection
            val cleared = DataKache.storageMode.databaseService.clear(this)

            // Clear the in-memory cache
            cacheMap.clear()

            getLoggerInternal().info(
                "Cleared all documents from cache: $cacheName ($cleared documents)",
            )
            return@wrap cleared
        }
    }

    // ------------------------------------------------------------ //
    //                       Extra CRUD Methods                     //
    // ------------------------------------------------------------ //
    override suspend fun readFromDatabase(key: K): OptionalResult<D> {
        return DbReadResultHandler.wrap {
            val doc = DataKache.storageMode.databaseService.read(this, key)
            if (doc != null) {
                // Cache the document if it was found
                cacheContentOnlyInternal(doc, log = true)
            }
            return@wrap doc
        }
    }

    override suspend fun readAllFromDatabase(): DefiniteResult<Flow<D>> =
        DbReadAllResultHandler.wrap {
        DataKache.storageMode.databaseService.readAll(this).map {
            // Cache each document as it is read from the database
            cacheContentOnlyInternal(it, log = true)
            it
        }
    }

    override suspend fun readSizeFromDatabase(): DefiniteResult<Long> =
        DbSizeResultHandler.wrap {
        DataKache.storageMode.databaseService.size(this)
    }

    override suspend fun hasKeyInDatabase(key: K): DefiniteResult<Boolean> =
        DbHasKeyResultHandler.wrap {
        DataKache.storageMode.databaseService.hasKey(this, key)
    }

    override suspend fun readKeysFromDatabase(): DefiniteResult<Flow<K>> =
        DbReadKeysResultHandler.wrap {
        DataKache.storageMode.databaseService.readKeys(this)
    }

    // ------------------------------------------------------------ //
    //                         Unique Indexes                       //
    // ------------------------------------------------------------ //
    override suspend fun <T> registerUniqueIndex(index: DocUniqueIndex<K, D, T>): DefiniteResult<Unit> {
        getLoggerInternal().debug("Registering unique index: ${index.fieldName} for cache: $cacheName")
        return DbRegisterUniqueIndexResultHandler.wrap {
            DataKache.storageMode.databaseService.registerUniqueIndex(this, index)
        }
    }

    override fun <T> readByUniqueIndex(index: DocUniqueIndex<K, D, T>, value: T): OptionalResult<D> {
        return ReadUniqueIndexResultHandler.wrap {
            // Read from cache trying to find the first document that matches the unique index value
            return@wrap cacheMap.values.firstOrNull { doc ->
                index.equals(index.extractValue(doc), value)
            }
        }
    }

    override suspend fun <T> readByUniqueIndexFromDatabase(
        index: DocUniqueIndex<K, D, T>,
        value: T,
    ): OptionalResult<D> =
        DbReadUniqueIndexResultHandler.wrap {
        DataKache.storageMode.databaseService.readByUniqueIndex(this, index, value)
    }

    // ------------------------------------------------------------ //
    //                     Internal Cache Methods                   //
    // ------------------------------------------------------------ //
    private companion object {
        /** Keys remembered after removal, so a late event for them is still refused. */
        const val TOMBSTONE_LIMIT = 10_000

        const val LOAD_FACTOR = 0.75f
    }

    /**
     * The position in the database's ordering that each key's cached state was taken from.
     *
     * Entries outlive the document. A key removed from [cacheMap] keeps its entry, because that is
     * what a late event for the key is compared against; forgetting it immediately would let the
     * event apply and put a deleted document back.
     */
    private val appliedAt = ConcurrentHashMap<K, OperationTime>()

    /**
     * Keys no longer in [cacheMap] whose position is still remembered, in removal order, so
     * [appliedAt] cannot grow without limit.
     *
     * Guarded by [tombstoneLock] rather than by a synchronized wrapper, because eviction has to
     * touch [appliedAt] and doing that while holding the tombstone monitor inverts the lock order
     * against [applyIfNewer], which holds an [appliedAt] bin lock. Hash-colliding keys could then
     * deadlock. The evicted key is therefore collected under the lock and forgotten outside it.
     */
    private val tombstones = LinkedHashMap<K, Unit>()

    private val tombstoneLock = Any()

    /** Remembers [key]'s position after removal, evicting the oldest once past the limit. */
    private fun rememberTombstone(key: K) {
        val evicted: K? =
            synchronized(tombstoneLock) {
                tombstones[key] = Unit
                if (tombstones.size > TOMBSTONE_LIMIT) {
                    val eldest = tombstones.keys.first()
                    tombstones.remove(eldest)
                    eldest
                } else {
                    null
                }
            }
        // Outside the monitor: see the note above about lock ordering.
        evicted?.let { appliedAt.remove(it) }
    }

    private fun forgetTombstone(key: K) {
        synchronized(tombstoneLock) { tombstones.remove(key) }
    }

    /**
     * Applies [action] only if [at] is strictly newer than the state the cache already holds for
     * [key], and records the new position atomically with it.
     *
     * Strictly newer rather than newer-or-equal makes redelivery a no-op: a change stream that
     * reconnects resumes from a token and can deliver an event that was already applied.
     *
     * The comparison and the mutation happen inside one [ConcurrentHashMap.compute], which holds the
     * per-key lock, so no other application of the same key can interleave between them.
     */
    private fun applyIfNewer(key: K, at: OperationTime, action: () -> Unit): Boolean {
        var applied = false
        appliedAt.compute(key) { _, current ->
            if (current != null && at <= current) {
                current
            } else {
                action()
                applied = true
                at
            }
        }
        return applied
    }

    @ApiStatus.Internal
    override fun cacheInternal(doc: D, at: OperationTime, log: Boolean) {
        doc.initializeInternal(this)
        val applied = applyIfNewer(doc.key, at) { cacheMap[doc.key] = doc }
        if (applied) forgetTombstone(doc.key)
        if (log && applied) {
            getLoggerInternal().debug("Cached document: ${doc.key}")
        } else if (!applied) {
            getLoggerInternal().debug("Refused stale state for ${doc.key} at $at")
        }
    }

    @ApiStatus.Internal
    override fun cacheContentOnlyInternal(doc: D, log: Boolean) {
        doc.initializeInternal(this)

        // Only populate a key that has no position yet. A read carries no operation time of its
        // own: the time it was performed at is later than the commit time of the data it returned,
        // so recording it would over-claim and refuse a genuinely newer event. Writing the content
        // without a position is worse still, because a slow read could overwrite newer state while
        // appliedAt kept the newer time, and the event that would repair it would then be refused.
        //
        // The caller still receives the document it read; only the cache side effect is skipped.
        var cached = false
        appliedAt.compute(doc.key) { _, current ->
            if (current == null) {
                cacheMap[doc.key] = doc
                cached = true
            }
            current
        }
        if (cached && log) {
            getLoggerInternal().debug("Cached document from a read: ${doc.key}")
        }
    }

    @ApiStatus.Internal
    override fun uncacheInternal(doc: D, at: OperationTime): Boolean = uncacheInternal(doc.key, at)

    @ApiStatus.Internal
    override fun uncacheInternal(key: K, at: OperationTime): Boolean {
        var removed = false
        val applied = applyIfNewer(key, at) { removed = cacheMap.remove(key) != null }
        if (applied) rememberTombstone(key)
        return removed
    }

    /**
     * @return The same [doc] for chaining.
     */
    @ApiStatus.Internal
    @Throws(DuplicateDocumentKeyException::class, DuplicateUniqueIndexException::class)
    suspend fun insertDocumentInternal(doc: D): D {
        // The database service caches the document itself, using the operation time of the
        // session that performed the write. Caching here as well would apply it without one.
        DataKache.storageMode.databaseService.insert(this, doc)
        return doc
    }

    /**
     * @return The same [update] document for chaining.
     */
    @ApiStatus.Internal
    @Throws(DocumentNotFoundException::class)
    suspend fun replaceDocumentInternal(key: K, update: D): D {
        // Insert the document in the database
        DataKache.storageMode.databaseService.replace(this, key, update)
        // Cache the document in memory
        return update
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
    override fun areChangeStreamJobsRunning(): Boolean = changeStreamManager?.areJobsActive() ?: false

    private suspend fun loadAllIntoCache(at: OperationTime?) =
        withContext(Dispatchers.IO) {
        val documents = DataKache.storageMode.databaseService.readAll(this@DocCacheImpl)
        documents.collect { doc ->
            // The preload reflects the state at the operation time captured before it began, and
            // the change stream starts from that same point, so the two meet without a gap.
            if (at != null) cacheInternal(doc, at, log = false) else cacheContentOnlyInternal(doc, log = false)
        }
    }

    private suspend fun startChangeStreamListener(operationTime: OperationTime?) {
        // Create the change stream manager through the database service
        DataKache.storageMode.databaseService
            .createChangeStreamManager(
                this@DocCacheImpl,
                createChangeEventHandler(),
            ).also {
                changeStreamManager = it

                // Start the change stream with pre-captured operation time to prevent timing gaps
                it.start(operationTime)

                getLoggerInternal().debug(
                    "Started change stream listener for cache: $cacheName with operation time: $operationTime",
                )
            }
    }

    private fun createChangeEventHandler(): ChangeEventHandler<K, D> =
        object : ChangeEventHandler<K, D> {
        override suspend fun onDocumentChanged(doc: D, changeType: ChangeDocumentType, at: OperationTime) {
            val name = this@DocCacheImpl.cacheName
            val key = this@DocCacheImpl.keyToString(doc.key)

            when (changeType) {
                ChangeDocumentType.INSERT -> {
                    // METRICS
                    DataKacheMetrics.getReceiversInternal().forEach {
                        it.onChangeStreamInsert(name, key)
                    }

                    cacheInternal(doc, at, log = false)
                    getLoggerInternal().debug("Cached Document From INSERT: ${doc.key}")
                }

                ChangeDocumentType.REPLACE -> {
                    // METRICS
                    DataKacheMetrics.getReceiversInternal().forEach {
                        it.onChangeStreamReplace(name, key)
                    }

                    cacheInternal(doc, at, log = false)
                    getLoggerInternal().debug("Cached Document From REPLACE: ${doc.key}")
                }

                ChangeDocumentType.UPDATE -> {
                    // METRICS
                    DataKacheMetrics.getReceiversInternal().forEach {
                        it.onChangeStreamUpdate(name, key)
                    }

                    cacheInternal(doc, at, log = false)
                    getLoggerInternal().debug("Cached Document From UPDATE: ${doc.key}")
                }
            }
        }

        override suspend fun onDocumentDeleted(keyString: String, at: OperationTime) {
            val key: K = this@DocCacheImpl.keyFromString(keyString)

            // METRICS
            val name = this@DocCacheImpl.cacheName
            DataKacheMetrics.getReceiversInternal().forEach {
                it.onChangeStreamDelete(name, keyString)
            }

            val removed = uncacheInternal(key, at)
            if (removed) {
                getLoggerInternal().debug("Uncached Document From DELETE: $key")
            }
        }

        override suspend fun onCollectionDropped() {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach {
                it.onChangeStreamDrop(cacheName)
            }

            // Collection was dropped - clear the entire cache
            val cachedCount = cacheMap.size
            cacheMap.clear()
            getLoggerInternal().warn(
                "Collection dropped - cleared cache ($cachedCount documents) for: $cacheName",
            )
        }

        override suspend fun onCollectionRenamed() {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach {
                it.onChangeStreamRename(cacheName)
            }

            // Collection was renamed - clear the cache as we're no longer tracking the correct collection
            val cachedCount = cacheMap.size
            cacheMap.clear()
            getLoggerInternal().warn(
                "Collection renamed - cleared cache ($cachedCount documents) for: $cacheName. " +
                    "Cache may need to be reregistered with new collection name.",
            )
        }

        override suspend fun onDatabaseDropped() {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach {
                it.onChangeStreamDropDatabase(cacheName)
            }

            // Database was dropped - this is a fatal error requiring cache shutdown
            getLoggerInternal().error(
                "Database '$databaseName' was dropped - " +
                    "initiating emergency cache shutdown for: $cacheName",
            )

            try {
                // Clear cache immediately
                cacheMap.clear()

                // Attempt graceful shutdown in background
                // Note: This is an emergency situation, so we don't wait for completion
                kotlinx.coroutines.CoroutineScope(Dispatchers.IO).launch {
                    try {
                        shutdown()
                    } catch (e: Exception) {
                        getLoggerInternal().error(
                            e,
                            "Error during emergency shutdown: $cacheName",
                        )
                    }
                }
            } catch (e: Exception) {
                getLoggerInternal().error(
                    e,
                    "Error during DROP_DATABASE handling: $cacheName",
                )
            }
        }

        override suspend fun onChangeStreamInvalidated() {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach {
                it.onChangeStreamInvalidate(cacheName)
            }

            // Change stream was invalidated - log error
            getLoggerInternal().error(
                "Change stream invalidated for cache: $cacheName. " +
                    "This may indicate a significant database event. " +
                    "The stream will attempt to reconnect automatically.",
            )

            // The change stream manager should handle reconnection automatically,
            // but we log this as a critical event for monitoring
            getLoggerInternal().warn(
                "Cache $cacheName may be in an inconsistent state due to stream invalidation. " +
                    "Consider manual verification if issues persist.",
            )
        }

        override suspend fun onUnknownOperation() {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach {
                it.onChangeStreamUnknown(cacheName)
            }

            getLoggerInternal().warn(
                "Unknown operation type received for cache: $cacheName. " +
                    "This may indicate a new MongoDB operation type that needs to be handled.",
            )
        }

        override suspend fun onConnected() {
            getLoggerInternal().debug("Change stream connected for cache: $cacheName")
        }

        override suspend fun onDisconnected() {
            getLoggerInternal().warn("Change stream disconnected for cache: $cacheName")
        }
    }
}
