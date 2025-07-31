@file:Suppress("unused")

package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.api.doc.GenericDoc
import com.jakemoore.datakache.api.logging.DefaultCacheLogger
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.handler.CreateResultHandler
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
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //

    override suspend fun create(key: String, initializer: (D) -> D): DefiniteResult<D> {
        return CreateResultHandler.wrap {
            // Create a new instance in modifiable state
            val instantiated: D = instantiator(key, 0L)
            instantiated.initializeInternal(this)

            // Allow caller to initialize the document with starter data
            val doc: D = initializer(instantiated)
            require(doc.key == key) {
                "The key of the GenericDoc must not change during initializer. Expected: $key, Actual: ${doc.key}"
            }
            assert(doc.version == 0L) {
                "The version of the GenericDoc must not change during initializer. Expected: 0L, Actual: ${doc.version}"
            }
            doc.initializeInternal(this)

            // Access internal method to save and cache the document
            return@wrap this.insertDocumentInternal(doc)
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
