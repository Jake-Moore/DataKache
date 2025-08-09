@file:Suppress("unused")

package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.doc.GenericDoc
import com.jakemoore.datakache.api.exception.update.IllegalDocumentKeyModificationException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentVersionModificationException
import com.jakemoore.datakache.api.logging.DefaultCacheLogger
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.handler.CreateGenericDocResultHandler
import com.jakemoore.datakache.api.result.handler.DeleteResultHandler
import java.util.UUID

abstract class GenericDocCache<D : GenericDoc<D>>(
    cacheName: String,
    registration: DataKacheRegistration,
    docClass: Class<D>,
    logger: (String) -> LoggerService = { cacheName -> DefaultCacheLogger(cacheName) },

    /**
     * @param UUID the unique identifier for the document.
     * @param Long the version of the document.
     */
    val instantiator: (String, Long) -> D,

) : DocCacheImpl<String, D>(cacheName, registration, docClass, logger) {

    // ------------------------------------------------------------ //
    //                         Service Methods                      //
    // ------------------------------------------------------------ //
    override suspend fun shutdownSuper(): Boolean {
        // Nothing to do here, no special shutdown logic for GenericDocCache
        return true
    }

    // ------------------------------------------------------------ //
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //

    override suspend fun create(key: String, initializer: (D) -> D): DefiniteResult<D> {
        return CreateGenericDocResultHandler.wrap {
            val namespace = this.getKeyNamespace(key)

            // Create a new instance in modifiable state
            val instantiated: D = instantiator(key, 0L)
            instantiated.initializeInternal(this)

            // Allow caller to initialize the document with starter data
            val doc: D = initializer(instantiated)

            // Require the Key to stay the same
            if (doc.key != key) {
                val foundKeyString = this.keyToString(doc.key)
                val expectedKeyString = this.keyToString(key)
                throw IllegalDocumentKeyModificationException(
                    namespace,
                    foundKeyString,
                    expectedKeyString,
                )
            }

            // Require the Version to stay the same
            val expectedVersion = 0L
            val foundVersion = doc.version
            if (foundVersion != expectedVersion) {
                throw IllegalDocumentVersionModificationException(namespace, foundVersion, expectedVersion)
            }

            doc.initializeInternal(this)
            // Access internal method to save and cache the document
            return@wrap this.insertDocumentInternal(doc, force = true)
        }
    }

    /**
     * Creates a new document in the cache (backed by a database object) with a random key (UUID string).
     *
     * See [create] for creating a document with a specific key.
     *
     * @param initializer A callback function for initializing the document with starter data.
     *
     * @return A [DefiniteResult] containing the document, or the exception if the document could not be created.
     */
    suspend fun createRandom(initializer: (D) -> D = { it }): DefiniteResult<D> {
        return create(UUID.randomUUID().toString(), initializer)
    }

    /**
     * Deletes a document from the cache and the backing database.
     *
     * @param key The unique key of the document to be deleted.
     *
     * @return A [DefiniteResult] indicating if the document was found and deleted. (false = not found)
     */
    override suspend fun delete(key: String): DefiniteResult<Boolean> {
        return DeleteResultHandler.wrap {
            val found = cacheMap.remove(key) != null
            DataKache.storageMode.databaseService.delete(this, key)
            cacheMap.remove(key)

            // This method boolean is whether the document was found in the cache
            //   false is okay, just indicates that the document was not cached
            return@wrap found
        }
    }

    // ------------------------------------------------------------ //
    //                    Key Manipulation Methods                  //
    // ------------------------------------------------------------ //
    override fun keyFromString(string: String): String {
        return string
    }
    override fun keyToString(key: String): String {
        return key
    }
}
