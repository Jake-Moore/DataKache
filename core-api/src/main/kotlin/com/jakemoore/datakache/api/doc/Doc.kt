package com.jakemoore.datakache.api.doc

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.coroutines.DataKacheScope
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.RejectableResult
import org.jetbrains.annotations.ApiStatus

/**
 * Represents an IMMUTABLE 'document' in the DataKache system.
 * This is some piece of data bound to a given cache by its key [K].
 *
 * CRUD operations are possible on this document,
 * however its state is IMMUTABLE and updates will return a new instance of the document.
 *
 * The cache will always reflect the latest version of the document.
 *
 * Generics:
 * - [K] - The type of the unique identifier for this document (must be hashable and comparable).
 * - [D] - The type of the document itself, which must extend [Doc].
 */
@Suppress("unused")
interface Doc<K : Any, D : Doc<K, D>> : DataKacheScope {
    enum class Status {
        /**
         * The document version matches the cache version. (assumed to be up-to-date)
         */
        FRESH,

        /**
         * The document version does not match the cache version.
         */
        STALE,

        /**
         * The document is not present in the cache. (assumed to be deleted)
         */
        DELETED,

        /**
         * The document is not attached to any cache. (improperly initialized? manually created?)
         */
        DETACHED,
    }

    // ------------------------------------------------------------ //
    //                         Properties                           //
    // ------------------------------------------------------------ //
    /**
     * The unique identifier of this [Doc] (type [K]).
     *
     * This 'key' must be unique, and [K] must be hashable and comparable.
     */
    val key: K

    /**
     * The version of this [Doc]. This version is IMMUTABLE for this object's lifecycle.
     *
     * This version can be used to determine how up-to-date this document is.
     *
     * NOTE:
     * - If this version does not match the version of the document in cache, then this document is considered stale.
     * - This (stale) document can still be used, and updates can be made, but they will require additional steps
     * to ensure consistency as your update gets applied on top of the latest document version.
     */
    val version: Long

    // ------------------------------------------------------------ //
    //                          API Methods                         //
    // ------------------------------------------------------------ //
    /**
     * @return The [DocCache] associated with this [Doc]. (This doc can be found in this cache, unless deleted)
     */
    fun getDocCache(): DocCache<K, D>

    @ApiStatus.Internal
    fun hasDocCacheInternal(): Boolean

    /**
     * Returns the status of this document based on its version and the current state of the cache.
     * Possible Status Values:
     * - [Status.FRESH]: Cache contains the exact same document and version. Data is up-to-date.
     * - [Status.STALE]: Cache contains a different version of the document. Data is outdated.
     * - [Status.DELETED]: Cache does not contain the document at all. Data is considered deleted.
     * - [Status.DETACHED]: Document is not attached to any cache. This can happen if the document was created manually
     *   or if it was not properly initialized with a cache.
     */
    fun getStatus(): Status {
        if (!hasDocCacheInternal()) return Status.DETACHED
        return this.getDocCache().getStatus(this.key, this.version)
    }

    // ------------------------------------------------------------ //
    //                       Data Class Helpers                     //
    // ------------------------------------------------------------ //
    // We do not have access to the top level data class, so we ask
    //  that the doc provide some methods that allow access to the data class copy
    /**
     * Produces a new instance of this document with its version updated.
     *
     * Top-level data classes should override this by invoking their generated
     * `copy(version = version)` method.
     *
     * @param version The new version for the copied document.
     * @return A new instance of the document with the updated version.
     */
    fun copyHelper(version: Long): D

    // ------------------------------------------------------------ //
    //                      Internal API Methods                    //
    // ------------------------------------------------------------ //
    @ApiStatus.Internal
    fun initializeInternal(cache: DocCache<K, D>)

    // ------------------------------------------------------------ //
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //
    /**
     * Modify this document (both cache and database will be updated).
     *
     * @return A [DefiniteResult] containing the updated document, or an exception if the document could not be updated.
     */
    @Throws(DocumentNotFoundException::class)
    suspend fun update(updateFunction: (D) -> D): DefiniteResult<D> {
        return this.getDocCache().update(this.key, updateFunction)
    }

    /**
     * Modify this document, allowing the operation to gracefully be rejected within the [updateFunction].
     *
     * Within the [updateFunction], you can throw a [RejectUpdateException] to cancel the update operation. The
     * [RejectableResult] will then indicate that the update was rejected, and no modifications were made.
     *
     * @return The [RejectableResult] containing:
     * - the updated document if the update was successful
     * - an exception if the update failed
     * - or a rejection state if the update was rejected by the [updateFunction]
     */
    @Throws(DocumentNotFoundException::class)
    suspend fun updateRejectable(updateFunction: (D) -> D): RejectableResult<D> {
        return this.getDocCache().updateRejectable(this.key, updateFunction)
    }

    /**
     * See [DocCache.delete] for details.
     */
    suspend fun delete(): DefiniteResult<Boolean> {
        return this.getDocCache().delete(this.key)
    }
}
