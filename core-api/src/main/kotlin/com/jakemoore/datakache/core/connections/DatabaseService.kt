package com.jakemoore.datakache.core.connections

import com.google.common.cache.Cache
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiver
import com.jakemoore.datakache.core.Service
import com.jakemoore.datakache.core.connections.changes.ChangeEventHandler
import com.jakemoore.datakache.core.connections.changes.ChangeStreamManager
import com.mongodb.DuplicateKeyException
import kotlinx.coroutines.flow.Flow

// TODO this file is part of the generic database-agnostic API but it uses the mongodb DuplicateKeyException
//  a project-wide scan is needed to replace all generic uses with a version of our own exception
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
     * Insert the given document to the database.
     *
     * Will not overwrite an existing document with the same key, throwing a [DuplicateKeyException] in that case.
     */
    @Throws(DuplicateKeyException::class)
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
    protected abstract suspend fun <K : Any, D : Doc<K, D>> insertInternal(
        docCache: DocCache<K, D>,
        doc: D,
    )

    /**
     * Update the given document in the database using the provided update function.
     *
     * Requires that the document exists in the database, otherwise a
     * [com.jakemoore.datakache.api.exception.DocumentNotFoundException] will be thrown
     */
    @Throws(DocumentNotFoundException::class)
    suspend fun <K : Any, D : Doc<K, D>> update(
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D,
    ): D {
        try {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseUpdate)

            return updateInternal(docCache, doc, updateFunction)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseUpdateFail)
            throw e
        }
    }
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
     * @throws NoSuchElementException if the document with the given key does not exist.
     */
    @Throws(NoSuchElementException::class)
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
    protected abstract suspend fun <K : Any, D : Doc<K, D>> replaceInternal(
        docCache: DocCache<K, D>,
        key: K,
        update: D,
    )

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
}
