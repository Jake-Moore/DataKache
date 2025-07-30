package com.jakemoore.datakache.core.connections

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.core.Service

/**
 * The set of all methods that a Database service must implement. This includes all CRUD operations DataKache needs.
 */
internal interface DatabaseService : LoggerService, Service {
    /**
     * The average round-trip ping to the storage service (database) in nanoseconds.
     */
    val averagePingNanos: Long

    /**
     * Save the given document to the database.
     *
     * Always succeeds (unless an exception is thrown) and will overwrite any existing document with the same key.
     */
    suspend fun <K : Any, D : Doc<K, D>> save(docCache: DocCache<K, D>, doc: D)

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
}
