package com.jakemoore.datakache.core.connections

import com.google.common.cache.Cache
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.logging.LoggerService
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
internal interface DatabaseService : LoggerService, Service {
    /**
     * The average round-trip ping to the storage service (database) in nanoseconds.
     */
    val averagePingNanos: Long

    /**
     * A map of server addresses (host:port) to their last ping time in nanoseconds.
     */
    val serverPingMap: Cache<String, Long>

    /**
     * Insert the given document to the database.
     *
     * Will not overwrite an existing document with the same key, throwing a [DuplicateKeyException] in that case.
     */
    @Throws(DuplicateKeyException::class)
    suspend fun <K : Any, D : Doc<K, D>> insert(docCache: DocCache<K, D>, doc: D)

    /**
     * Update the given document in the database using the provided update function.
     *
     * Requires that the document exists in the database, otherwise a
     * [com.jakemoore.datakache.api.exception.DocumentNotFoundException] will be thrown
     */
    @Throws(DocumentNotFoundException::class)
    suspend fun <K : Any, D : Doc<K, D>> update(docCache: DocCache<K, D>, doc: D, updateFunction: (D) -> D): D

    /**
     * Reads a document from the database by its [key].
     *
     * @return The document [D] if it exists, or null if it does not.
     */
    suspend fun <K : Any, D : Doc<K, D>> read(docCache: DocCache<K, D>, key: K): D?

    /**
     * Remove the document with the given [key] from the database.
     *
     * @return True if the document was successfully deleted, false if it did not exist.
     */
    suspend fun <K : Any, D : Doc<K, D>> delete(docCache: DocCache<K, D>, key: K): Boolean

    /**
     * @return If the database service is finished starting up and is ready to accept requests.
     */
    fun isDatabaseReadyForWrites(): Boolean

    /**
     * Read all documents from the given [docCache] as a kotlin [Flow].
     */
    suspend fun <K : Any, D : Doc<K, D>> readAll(docCache: DocCache<K, D>): Flow<D>

    /**
     * Gets the current operation time from the database to prevent timing gaps
     * in change stream initialization. The returned object is database-specific
     * and should be passed to createChangeStreamManager if timing gap prevention is needed.
     *
     * @return A database-specific timestamp object, or null if not supported
     */
    suspend fun getCurrentOperationTime(): Any?

    /**
     * Creates a change stream manager for the given [docCache] with the specified [eventHandler].
     *
     * @param docCache The document cache to create a change stream for
     * @param eventHandler The event handler to process change stream events
     * @return A change stream manager instance
     */
    suspend fun <K : Any, D : Doc<K, D>> createChangeStreamManager(
        docCache: DocCache<K, D>,
        eventHandler: ChangeEventHandler<K, D>
    ): ChangeStreamManager<K, D>
}
