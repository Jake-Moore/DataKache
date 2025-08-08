package com.jakemoore.datakache.core.connections

import com.google.common.cache.Cache
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
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiver
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.core.Service
import com.jakemoore.datakache.core.connections.changes.ChangeEventHandler
import com.jakemoore.datakache.core.connections.changes.ChangeStreamManager
import com.jakemoore.datakache.core.connections.queues.UpdateQueueManager
import kotlinx.coroutines.flow.Flow

/**
 * The set of all methods that a Database service must implement. This includes all CRUD operations DataKache needs.
 */
@Suppress("unused")
internal abstract class DatabaseService : LoggerService, Service {
    /**
     * The average ROUND-TRIP ping to the storage service (database) in nanoseconds.
     */
    abstract val averagePingNanos: Long

    /**
     * A map of server addresses (host:port) to their last ROUND-TRIP ping time in nanoseconds.
     */
    abstract val serverPingMap: Cache<String, Long>

    /**
     * Manager for per-document update queues to eliminate database-level conflicts.
     */
    private val updateQueueManager = UpdateQueueManager(this)

    // ------------------------------------------------------------ //
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //
    /**
     * Insert the given document to the database.
     *
     * Will not overwrite an existing document. Insertions that violate a primary key will throw:
     * - [DuplicateDocumentKeyException]
     * Insertions that violate a unique index will throw:
     * - [DuplicateUniqueIndexException]
     */
    @Throws(DuplicateDocumentKeyException::class, DuplicateUniqueIndexException::class)
    suspend fun <K : Any, D : Doc<K, D>> insert(
        docCache: DocCache<K, D>,
        doc: D,
    ) {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseInsert)

            return insertInternal(docCache, doc)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseInsertFail)
            throw e
        }
    }

    @Throws(DuplicateDocumentKeyException::class, DuplicateUniqueIndexException::class)
    protected abstract suspend fun <K : Any, D : Doc<K, D>> insertInternal(
        docCache: DocCache<K, D>,
        doc: D,
    )

    /**
     * Update the given document in the database using the provided update function.
     *
     * This method now uses a per-document queue system to eliminate database-level conflicts
     * and improve FIFO ordering of updates to the same document.
     *
     * The following exceptions may be thrown during the update:
     * - [DocumentNotFoundException]: if the document does not exist in the database.
     * - [DuplicateUniqueIndexException]: if the update violates a unique index constraint.
     * - [TransactionRetriesExceededException]: if the update exceeds the maximum number of retries.
     * - [DocumentUpdateException]: if the update function breaks a convention or fails.
     * - [InvalidDocCopyHelperException]: if the document copy helper is invalid.
     * - [UpdateFunctionReturnedSameInstanceException]: if the update function does not change the document
     * - [IllegalDocumentKeyModificationException]: if the update function modifies the document key.
     * - [IllegalDocumentVersionModificationException]: if the update function modifies the document version.
     * - [RejectUpdateException]: if the update is rejected by the update function.
     */
    @Throws(
        DocumentNotFoundException::class, DuplicateUniqueIndexException::class,
        TransactionRetriesExceededException::class, DocumentUpdateException::class,
        InvalidDocCopyHelperException::class, UpdateFunctionReturnedSameInstanceException::class,
        IllegalDocumentKeyModificationException::class, IllegalDocumentVersionModificationException::class,
        RejectUpdateException::class,
    )
    suspend fun <K : Any, D : Doc<K, D>> update(
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D,
    ): D {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseUpdate)

            // Use the queue system to serialize updates to the same document
            // This eliminates database-level conflicts and improves FIFO ordering
            val deferred = updateQueueManager.enqueueUpdate(
                docCache = docCache,
                doc = doc,
                updateFunction = updateFunction,
                updateExecutor = { cache, document, function ->
                    updateInternal(cache, document, function)
                }
            )

            return deferred.await()
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseUpdateFail)
            throw e
        }
    }

    @Throws(
        DocumentNotFoundException::class, DuplicateUniqueIndexException::class,
        TransactionRetriesExceededException::class, DocumentUpdateException::class,
        InvalidDocCopyHelperException::class, UpdateFunctionReturnedSameInstanceException::class,
        IllegalDocumentKeyModificationException::class, IllegalDocumentVersionModificationException::class,
        RejectUpdateException::class,
    )
    protected abstract suspend fun <K : Any, D : Doc<K, D>> updateInternal(
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D,
    ): D

    /**
     * Reads a document from the database by its [key].
     *
     * @return The document [D] if it exists, or null if it does not.
     */
    suspend fun <K : Any, D : Doc<K, D>> read(
        docCache: DocCache<K, D>,
        key: K,
    ): D? {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseRead)

            return readInternal(docCache, key)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseReadFail)
            throw e
        }
    }
    protected abstract suspend fun <K : Any, D : Doc<K, D>> readInternal(
        docCache: DocCache<K, D>,
        key: K,
    ): D?

    /**
     * Remove the document with the given [key] from the database.
     *
     * @return True if the document was successfully deleted, false if it did not exist.
     */
    suspend fun <K : Any, D : Doc<K, D>> delete(
        docCache: DocCache<K, D>,
        key: K,
    ): Boolean {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseDelete)

            return deleteInternal(docCache, key)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseDeleteFail)
            throw e
        }
    }
    protected abstract suspend fun <K : Any, D : Doc<K, D>> deleteInternal(
        docCache: DocCache<K, D>,
        key: K,
    ): Boolean

    /**
     * Read all documents from the given [docCache] as a kotlin [Flow].
     */
    suspend fun <K : Any, D : Doc<K, D>> readAll(
        docCache: DocCache<K, D>,
    ): Flow<D> {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseReadAll)

            return readAllInternal(docCache)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseReadAllFail)
            throw e
        }
    }
    protected abstract suspend fun <K : Any, D : Doc<K, D>> readAllInternal(
        docCache: DocCache<K, D>,
    ): Flow<D>

    /**
     * Fetches the size (total count of all documents) of the given [docCache].
     */
    suspend fun <K : Any, D : Doc<K, D>> size(
        docCache: DocCache<K, D>,
    ): Long {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseSize)

            return sizeInternal(docCache)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseSizeFail)
            throw e
        }
    }
    protected abstract suspend fun <K : Any, D : Doc<K, D>> sizeInternal(
        docCache: DocCache<K, D>,
    ): Long

    /**
     * Checks if a document with the given [key] exists in the [docCache].
     *
     * @return True if the document exists, false otherwise.
     */
    suspend fun <K : Any, D : Doc<K, D>> hasKey(
        docCache: DocCache<K, D>,
        key: K,
    ): Boolean {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseHasKey)

            return hasKeyInternal(docCache, key)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseHasKeyFail)
            throw e
        }
    }
    protected abstract suspend fun <K : Any, D : Doc<K, D>> hasKeyInternal(
        docCache: DocCache<K, D>,
        key: K,
    ): Boolean

    /**
     * Clears the entire [docCache] from the database.
     *
     * @return The number of documents removed from the database.
     */
    suspend fun <K : Any, D : Doc<K, D>> clear(
        docCache: DocCache<K, D>,
    ): Long {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseClear)

            return clearInternal(docCache)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseClearFail)
            throw e
        }
    }
    protected abstract suspend fun <K : Any, D : Doc<K, D>> clearInternal(
        docCache: DocCache<K, D>,
    ): Long

    /**
     * Read all keys from the given [docCache] as a kotlin [Flow].
     */
    suspend fun <K : Any, D : Doc<K, D>> readKeys(
        docCache: DocCache<K, D>,
    ): Flow<K> {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseReadKeys)

            return readKeysInternal(docCache)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseReadKeysFail)
            throw e
        }
    }
    protected abstract suspend fun <K : Any, D : Doc<K, D>> readKeysInternal(
        docCache: DocCache<K, D>,
    ): Flow<K>

    /**
     * Fully overwrite and replace the document with the given [key] using the provided [update] document.
     *
     * This will replace the entire document, not just update specific fields.
     * - This function will NOT insert the document if the key does not already exist.
     *
     * @throws DocumentNotFoundException if the document with the given key does not exist.
     */
    @Throws(DocumentNotFoundException::class)
    suspend fun <K : Any, D : Doc<K, D>> replace(
        docCache: DocCache<K, D>,
        key: K,
        update: D,
    ) {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseReplace)

            return replaceInternal(docCache, key, update)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseReplaceFail)
            throw e
        }
    }

    @Throws(DocumentNotFoundException::class)
    protected abstract suspend fun <K : Any, D : Doc<K, D>> replaceInternal(
        docCache: DocCache<K, D>,
        key: K,
        update: D,
    )

    // ------------------------------------------------------------ //
    //                         Unique Indexes                       //
    // ------------------------------------------------------------ //
    /**
     * Register a custom index for this cache.
     *
     * This index will have uniqueness constraints enforced, similar to a superkey.
     */
    suspend fun <K : Any, D : Doc<K, D>, T> registerUniqueIndex(
        docCache: DocCache<K, D>,
        index: DocUniqueIndex<K, D, T>,
    ) {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onRegisterUniqueIndex)

            return registerUniqueIndexInternal(docCache, index)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onRegisterUniqueIndexFail)
            throw e
        }
    }
    protected abstract suspend fun <K : Any, D : Doc<K, D>, T> registerUniqueIndexInternal(
        docCache: DocCache<K, D>,
        index: DocUniqueIndex<K, D, T>,
    )

    /**
     * Attempts to read the document from the cache by a unique index. (ONLY checks cache)
     *
     * @param index The unique index previously registered on this cache.
     * @param value The value in the index to search for.
     *
     * @return The [OptionalResult] containing the document if found, or empty if it does not.
     */
    suspend fun <K : Any, D : Doc<K, D>, T> readByUniqueIndex(
        docCache: DocCache<K, D>,
        index: DocUniqueIndex<K, D, T>,
        value: T,
    ): D? {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseReadDocByUniqueIndex)

            return readByUniqueIndexInternal(docCache, index, value)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseReadDocByUniqueIndexFail)
            throw e
        }
    }
    protected abstract suspend fun <K : Any, D : Doc<K, D>, T> readByUniqueIndexInternal(
        docCache: DocCache<K, D>,
        index: DocUniqueIndex<K, D, T>,
        value: T,
    ): D?

    // ------------------------------------------------------------ //
    //                            MISC API                          //
    // ------------------------------------------------------------ //
    /**
     * @return If the database service is finished starting up and is ready to accept requests.
     */
    abstract fun isDatabaseReadyForWrites(): Boolean

    /**
     * Gets the current operation time from the database to prevent timing gaps
     * in change stream initialization. The returned object is database-specific
     * and should be passed to createChangeStreamManager if timing gap prevention is needed.
     *
     * @return A database-specific timestamp object, or null if not supported
     */
    abstract suspend fun getCurrentOperationTime(): Any?

    /**
     * Creates a change stream manager for the given [docCache] with the specified [eventHandler].
     *
     * @param docCache The document cache to create a change stream for
     * @param eventHandler The event handler to process change stream events
     * @return A change stream manager instance
     */
    abstract suspend fun <K : Any, D : Doc<K, D>> createChangeStreamManager(
        docCache: DocCache<K, D>,
        eventHandler: ChangeEventHandler<K, D>
    ): ChangeStreamManager<K, D>

    /**
     * Override shutdown to clean up the update queue manager.
     * Subclasses should call super.shutdown() in their implementation.
     */
    override suspend fun shutdown(): Boolean {
        try {
            updateQueueManager.shutdown()
            return true
        } catch (e: Exception) {
            error(e, "Failed to shutdown UpdateQueueManager")
            return false
        }
    }

    /**
     * Returns the number of active update queues for monitoring purposes.
     */
    fun getActiveUpdateQueuesCount(): Int = updateQueueManager.getActiveQueuesCount()
}
