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
import com.jakemoore.datakache.api.exception.update.UpdateQueueShutdownException
import com.jakemoore.datakache.api.exception.update.UpdateQueueStalledException
import com.jakemoore.datakache.api.exception.update.UpdateQueueTooDeepException
import com.jakemoore.datakache.api.index.DocUniqueIndex
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiver
import com.jakemoore.datakache.api.ordering.OperationTime
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.core.Service
import com.jakemoore.datakache.core.connections.changes.ChangeEventHandler
import com.jakemoore.datakache.core.connections.changes.ChangeStreamManager
import com.jakemoore.datakache.core.connections.queues.UpdateQueueManager
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.TimeSource

/**
 * The set of all methods that a Database service must implement. This includes all CRUD operations DataKache needs.
 */
@Suppress("unused")
internal abstract class DatabaseService :
    LoggerService,
    Service {
    /**
     * The average ROUND-TRIP ping to the storage service (database) in nanoseconds.
     */
    abstract val averagePingNanos: Long

    /**
     * A map of server addresses (host:port) to their last ROUND-TRIP ping time in nanoseconds.
     */
    abstract val serverPingMap: Cache<String, Long>

    private val updateQueueManagerDelegate =
        lazy {
            // This is a delegate to ensure the UpdateQueueManager is only initialized when needed
            UpdateQueueManager(this)
        }

    /**
     * Manager for per-document update queues to eliminate database-level conflicts.
     */
    private val updateQueueManager by updateQueueManagerDelegate

    /** Exposed so tests can enqueue through the real queue with a controlled executor. */
    internal val updateQueueManagerInternal: UpdateQueueManager get() = updateQueueManager

    private companion object {
        /** How many single-update budgets a caller waits behind a progressing queue. */
        const val CEILING_MULTIPLE = 4L
    }

    /**
     * The longest a single update may legitimately take on this backend, including whatever
     * retrying it does internally.
     *
     * Stated by the backend rather than chosen here, because only the backend knows its own retry
     * policy. A number picked independently of it stops being right the moment that policy changes,
     * and the failure is silent: healthy updates start being reported as wedged.
     */
    abstract val maxSingleUpdateMs: Long

    /**
     * Overrides [stallWindow] for tests, which cannot wait out a real backend's retry budget.
     */
    @Volatile
    internal var stallWindowMsOverride: Long? = null

    /**
     * The longest a **single** update may take before the queue behind it is called wedged.
     *
     * From outside a queue, one item that never returns and one item that is merely slow look the
     * same: neither completes. So a bound on one item is unavoidable, and this is it.
     *
     * **What matters is what it does NOT scale with.** Not queue depth, not contention, not the
     * number of writers. Those are what made the previous budget wrong: it multiplied a per-item
     * estimate by the depth, so a document with fifty writers needed fifty times the budget and got
     * an estimate derived from ping. Here, fifty queued updates each taking a second are fifty
     * healthy windows in a row, and only an update that takes longer than this on its own is a
     * fault.
     *
     * Derived from [maxSingleUpdateMs] so it always exceeds what it bounds.
     */
    private val stallWindow: Long get() = stallWindowMsOverride ?: maxSingleUpdateMs

    /**
     * How long a caller waits behind a queue that is progressing but never reaches them.
     *
     * A multiple of the single-update bound rather than a separate constant, so a backend with a
     * long retry budget does not get a ceiling it can cross while perfectly healthy.
     */
    @Volatile
    internal var queueCeilingMsOverride: Long? = null

    private val queueCeiling: Long get() = queueCeilingMsOverride ?: (maxSingleUpdateMs * CEILING_MULTIPLE)

    /**
     * Waits for [queued], failing only when the queue behind it is actually in trouble.
     *
     * **Two boundaries, because there are two faults and they need different answers.**
     *
     * A queue that completes nothing for [stallWindow] has an item that has taken longer than any
     * single update may. That is a fault at any depth, on any machine, because the bound is on one
     * item rather than on the queue.
     *
     * A queue that keeps completing but never reaches this caller within [queueCeiling] is receiving
     * work faster than it can finish it. Also a fault, but a different one, so it is a different
     * exception: the remedy is to write to that document less, not to look for a deadlock.
     */
    internal suspend fun <K : Any, D : Doc<K, D>> awaitUpdate(
        docCache: DocCache<K, D>,
        docKey: K,
        queued: UpdateQueueManager.QueuedUpdate<K, D>,
    ): D {
        val start = TimeSource.Monotonic.markNow()
        // No queue means the manager was shut down and the request failed without being enqueued,
        // so there is nothing to watch and the await below throws immediately.
        val queue = queued.queue ?: return queued.deferred.await()
        var lastCompleted = queue.getCompletedCount()

        while (true) {
            // Only the wait is abandoned here, never the update: the queue owns the deferred and
            // carries on regardless, so a window elapsing costs nothing but another look.
            val result = withTimeoutOrNull(stallWindow) { queued.deferred.await() }
            if (result != null) return result

            val completed = queue.getCompletedCount()
            val depth = queue.getTotalQueueSize()
            val namespace = docCache.getKeyNamespace(docKey)

            if (completed == lastCompleted) {
                DataKacheMetrics.receivers.forEach {
                    it.onUpdateQueueStalled(docCache.cacheName, docCache.keyToString(docKey), depth)
                }
                throw UpdateQueueStalledException(namespace, stallWindow, depth)
            }
            lastCompleted = completed

            val waited = start.elapsedNow().inWholeMilliseconds
            if (waited >= queueCeiling) {
                DataKacheMetrics.receivers.forEach {
                    it.onUpdateQueueTooDeep(docCache.cacheName, docCache.keyToString(docKey), waited, depth)
                }
                throw UpdateQueueTooDeepException(namespace, waited, depth)
            }
        }
    }

    // ------------------------------------------------------------ //
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //

    /**
     * Ensure the backing collection for the given [docCache] exists in the database.
     *
     * Creates the collection if it does not exist.
     */
    internal abstract suspend fun <K : Any, D : Doc<K, D>> ensureCollectionExists(docCache: DocCache<K, D>)

    /**
     * Insert the given document to the database.
     *
     * Will not overwrite an existing document. Insertions that violate a primary key will throw:
     * - [DuplicateDocumentKeyException]
     * Insertions that violate a unique index will throw:
     * - [DuplicateUniqueIndexException]
     */
    @Throws(DuplicateDocumentKeyException::class, DuplicateUniqueIndexException::class)
    suspend fun <K : Any, D : Doc<K, D>> insert(docCache: DocCache<K, D>, doc: D) {
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
    protected abstract suspend fun <K : Any, D : Doc<K, D>> insertInternal(docCache: DocCache<K, D>, doc: D)

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
     * - [UpdateQueueStalledException]: if the queue for this document stopped making progress.
     * - [UpdateQueueTooDeepException]: if the queue kept progressing but never reached this caller.
     * - [UpdateQueueShutdownException]: if the queue was shut down before the update finished.
     *
     * The last three leave the outcome **unknown** rather than failed, so do not blindly retry.
     */
    @Throws(
        DocumentNotFoundException::class, DuplicateUniqueIndexException::class,
        TransactionRetriesExceededException::class, DocumentUpdateException::class,
        InvalidDocCopyHelperException::class, UpdateFunctionReturnedSameInstanceException::class,
        IllegalDocumentKeyModificationException::class, IllegalDocumentVersionModificationException::class,
        RejectUpdateException::class, UpdateQueueStalledException::class,
        UpdateQueueTooDeepException::class, UpdateQueueShutdownException::class,
    )
    suspend fun <K : Any, D : Doc<K, D>> update(
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D,
        bypassValidation: Boolean = false,
    ): D {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseUpdate)

            // Use the queue system to serialize updates to the same document
            // This eliminates database-level conflicts and improves FIFO ordering
            val queued =
                updateQueueManager.enqueueUpdate(
                    docCache = docCache,
                    doc = doc,
                    updateFunction = updateFunction,
                    updateExecutor = ::updateInternal,
                    bypassValidation = bypassValidation,
                )

            return awaitUpdate(docCache, doc.key, queued)
        } catch (e: UpdateQueueStalledException) {
            // Named individually on purpose. Catching the whole DocumentUpdateException family here
            // would take the four validation failures that already existed out of
            // onDatabaseUpdateFail, which no test asserts on and nobody would notice until a
            // dashboard quietly under-counted.
            error(e.message)
            throw e
        } catch (e: UpdateQueueTooDeepException) {
            error(e.message)
            throw e
        } catch (e: UpdateQueueShutdownException) {
            // A queue fault rather than a database one, and the expected shape of teardown under
            // load, so counting it as an update failure would put a spike on every clean shutdown.
            error(e.message)
            throw e
        } catch (e: CancellationException) {
            throw e
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
        bypassValidation: Boolean,
    ): D

    /**
     * Reads a document from the database by its [key].
     *
     * @return The document [D] if it exists, or null if it does not.
     */
    suspend fun <K : Any, D : Doc<K, D>> read(docCache: DocCache<K, D>, key: K): D? {
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

    protected abstract suspend fun <K : Any, D : Doc<K, D>> readInternal(docCache: DocCache<K, D>, key: K): D?

    /**
     * Remove the document with the given [key] from the database.
     *
     * @return True if the document was successfully deleted, false if it did not exist.
     */
    suspend fun <K : Any, D : Doc<K, D>> delete(docCache: DocCache<K, D>, key: K): Boolean {
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

    protected abstract suspend fun <K : Any, D : Doc<K, D>> deleteInternal(docCache: DocCache<K, D>, key: K): Boolean

    /**
     * Read all documents from the given [docCache] as a kotlin [Flow].
     */
    suspend fun <K : Any, D : Doc<K, D>> readAll(docCache: DocCache<K, D>): Flow<D> {
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

    protected abstract suspend fun <K : Any, D : Doc<K, D>> readAllInternal(docCache: DocCache<K, D>): Flow<D>

    /**
     * Fetches the size (total count of all documents) of the given [docCache].
     */
    suspend fun <K : Any, D : Doc<K, D>> size(docCache: DocCache<K, D>): Long {
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

    protected abstract suspend fun <K : Any, D : Doc<K, D>> sizeInternal(docCache: DocCache<K, D>): Long

    /**
     * Checks if a document with the given [key] exists in the [docCache].
     *
     * @return True if the document exists, false otherwise.
     */
    suspend fun <K : Any, D : Doc<K, D>> hasKey(docCache: DocCache<K, D>, key: K): Boolean {
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

    protected abstract suspend fun <K : Any, D : Doc<K, D>> hasKeyInternal(docCache: DocCache<K, D>, key: K): Boolean

    /**
     * Clears the entire [docCache] from the database.
     *
     * @return The number of documents removed from the database.
     */
    suspend fun <K : Any, D : Doc<K, D>> clear(docCache: DocCache<K, D>): Long {
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

    protected abstract suspend fun <K : Any, D : Doc<K, D>> clearInternal(docCache: DocCache<K, D>): Long

    /**
     * Read all keys from the given [docCache] as a kotlin [Flow].
     */
    suspend fun <K : Any, D : Doc<K, D>> readKeys(docCache: DocCache<K, D>): Flow<K> {
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

    protected abstract suspend fun <K : Any, D : Doc<K, D>> readKeysInternal(docCache: DocCache<K, D>): Flow<K>

    /**
     * Fully overwrite and replace the document with the given [key] using the provided [update] document.
     *
     * This will replace the entire document, not just update specific fields.
     * - This function will NOT insert the document if the key does not already exist.
     *
     * @throws DocumentNotFoundException if the document with the given key does not exist.
     */
    @Throws(DocumentNotFoundException::class)
    suspend fun <K : Any, D : Doc<K, D>> replace(docCache: DocCache<K, D>, key: K, update: D) {
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
     * Reads the deployment's current position in its own ordering, before a preload begins, so the
     * change stream can be started from that same point and no write is missed between the two.
     *
     * This is a round trip. Prefer the operation time the session that performed a write already
     * reports, where one is available.
     *
     * @return The current position, or null if this deployment does not report one.
     */
    abstract suspend fun getCurrentOperationTime(): OperationTime?

    /**
     * Creates a change stream manager for the given [docCache] with the specified [eventHandler].
     *
     * @param docCache The document cache to create a change stream for
     * @param eventHandler The event handler to process change stream events
     * @return A change stream manager instance
     */
    abstract suspend fun <K : Any, D : Doc<K, D>> createChangeStreamManager(
        docCache: DocCache<K, D>,
        eventHandler: ChangeEventHandler<K, D>,
    ): ChangeStreamManager<K, D>

    /**
     * Override shutdown to clean up the update queue manager.
     * Subclasses should call super.shutdown() in their implementation.
     */
    override suspend fun shutdown(): Boolean {
        try {
            if (updateQueueManagerDelegate.isInitialized()) {
                updateQueueManager.shutdown()
            }
            return true
        } catch (e: Exception) {
            this.severe(throwable = e, msg = "Failed to shutdown UpdateQueueManager")
            return false
        }
    }

    /**
     * Returns the number of active update queues for monitoring purposes.
     */
    fun getActiveUpdateQueuesCount(): Int {
        if (!updateQueueManagerDelegate.isInitialized()) {
            // Queue system not online yet, then we have 0 active queues
            return 0
        }
        return updateQueueManager.getActiveQueuesCount()
    }
}
