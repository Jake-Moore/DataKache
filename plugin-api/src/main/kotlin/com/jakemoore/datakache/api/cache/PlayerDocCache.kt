@file:Suppress("unused")

package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.api.doc.PlayerDoc
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.DefiniteResult
import java.util.UUID

abstract class PlayerDocCache<D : PlayerDoc<D>>(
    registration: DataKacheRegistration,
    /**
     * @param UUID the unique identifier for the document.
     * @param Long the version of the document.
     * @param String the username of the player.
     */
    val instantiator: (UUID, Long, String) -> D,
    /**
     * The name of this cache, but also the desired name of the collection in the database.
     */
    cacheName: String,

) : DocCache<UUID, D> {

    // ----------------------------------------------------- //
    //                     CRUD Methods                      //
    // ----------------------------------------------------- //
    /**
     * Creates a new document in the cache (backed by a database object) with a random key.
     *
     * See [create] for creating a document with a specific key.
     *
     * @param initializer A callback function for initializing the document with starter data.
     *
     * @return A [DefiniteResult] containing the document, or the exception if the document could not be created.
     */
    abstract suspend fun createRandom(initializer: (D) -> D = { it }): DefiniteResult<D>
}
