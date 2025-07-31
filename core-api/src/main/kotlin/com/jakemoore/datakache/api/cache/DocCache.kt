package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.api.coroutines.DataKacheScope
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.api.result.RejectableResult
import kotlinx.serialization.KSerializer
import org.jetbrains.annotations.ApiStatus
import org.jetbrains.annotations.NonBlocking
import kotlin.reflect.KProperty

@Suppress("unused")
sealed interface DocCache<K : Any, D : Doc<K, D>> : DataKacheScope {
    // ------------------------------------------------------------ //
    //                     Kotlin Reflect Access                    //
    // ------------------------------------------------------------ //
    fun getKSerializer(): KSerializer<D>
    fun getKeyKProperty(): KProperty<K>
    fun getVersionKProperty(): KProperty<Long>

    // ------------------------------------------------------------ //
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //

    /**
     * Fetch a document from the cache by its key.
     *
     * DataKache will ensure that all documents from the backing database are loaded into this cache. Therefore,
     * reading from the cache is nearly instantaneous, and does not require any network calls.
     *
     * @param key The unique key of the document to be fetched.
     *
     * @return An [OptionalResult] containing the document if it exists, or empty if it does not.
     */
    @NonBlocking
    fun read(key: K): OptionalResult<D>

    /**
     * Modify a document by its key (both cache and database will be updated).
     *
     * @param key The unique key of the document to be updated.
     *
     * @return A [DefiniteResult] containing the updated document, or an exception if the document could not be updated.
     */
    @Throws(DocumentNotFoundException::class)
    suspend fun update(key: K, updateFunction: (D) -> D): DefiniteResult<D>

    /**
     * Modify a document by its key (both cache and database will be updated).
     *
     * @param doc The document to be updated (will be updated via its key).
     *
     * @return A [DefiniteResult] containing the updated document, or an exception if the document could not be updated.
     */
    @Throws(DocumentNotFoundException::class)
    suspend fun update(doc: D, updateFunction: (D) -> D): DefiniteResult<D> {
        return update(doc.key, updateFunction)
    }

    /**
     * Modify a document by its key, allowing the operation to gracefully be rejected within the [updateFunction].
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
    suspend fun updateRejectable(key: K, updateFunction: (D) -> D): RejectableResult<D>

    /**
     * Modify a document by its key, allowing the operation to gracefully be rejected within the [updateFunction].
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
    suspend fun updateRejectable(doc: D, updateFunction: (D) -> D): RejectableResult<D> {
        return updateRejectable(doc.key, updateFunction)
    }

    /**
     * Deletes a document from the cache and the backing database.
     *
     * @param key The unique key of the document to be deleted.
     *
     * @return A [DefiniteResult] indicating if the document was found and deleted. (false = not found)
     */
    suspend fun delete(key: K): DefiniteResult<Boolean>

    /**
     * Deletes a document from the cache and the backing database.
     *
     * @param doc The document to be deleted (deleted via its key).
     *
     * @return A [DefiniteResult] indicating if the document was found and deleted. (false = not found)
     */
    suspend fun delete(doc: D): DefiniteResult<Boolean> {
        return delete(doc.key)
    }

    // ------------------------------------------------------------ //
    //                       Extra CRUD Methods                     //
    // ------------------------------------------------------------ //
    /**
     * Fetch all documents from the cache.
     *
     * DataKache will ensure that all documents from the backing database are loaded into this cache. Therefore,
     * reading from the cache is nearly instantaneous, and does not require any network calls.
     *
     * @return A [Collection] of all documents in the cache.
     */
    fun readAll(): Collection<D>

    /**
     * Fetch all document keys from the cache.
     *
     * DataKache will ensure that all documents from the backing database are loaded into this cache. Therefore,
     * reading from the cache is nearly instantaneous, and does not require any network calls.
     *
     * @return A [Set] of all keys [K] in the cache.
     */
    fun getKeys(): Set<K>

    /**
     * Checks if a document with the given key is cached.
     *
     * DataKache will ensure that all documents from the backing database are loaded into this cache. Therefore,
     * reading from the cache is nearly instantaneous, and does not require any network calls.
     *
     * If this method returns false, the document does not exist in cache or database.
     */
    fun isCached(key: K): Boolean

    // ------------------------------------------------------------ //
    //                        DocCache Methods                      //
    // ------------------------------------------------------------ //
    /**
     * The name of this cache of documents. This name will be used to create a collection in the backing database.
     *
     * This cache name is **separate** from the database name inside your registration.
     */
    val cacheName: String

    /**
     * The DataKache registration that this cache is associated with.
     */
    val registration: DataKacheRegistration

    /**
     * The full name (including namespace) of the actual database this Cache is associated with.
     *
     * This name is formed by [com.jakemoore.datakache.api.DataKacheAPI.getFullDatabaseName].
     */
    val databaseName: String

    /**
     * The class of the document type [D] that this cache holds.
     */
    val docClass: Class<D>

    // ------------------------------------------------------------ //
    //                    Key Manipulation Methods                  //
    // ------------------------------------------------------------ //
    /**
     * Converts this key type [K] to a string representation.
     *
     * This is useful for logging, debugging, or any other purpose where a string representation of the key is needed.
     *
     * This operation is reversible by using [keyFromString].
     */
    fun keyToString(key: K): String

    /**
     * Converts a string representation of a key back to its original type [K].
     *
     * This is useful for converting keys stored in a string format back to their original type.
     *
     * This operation is reversible by using [keyToString].
     */
    fun keyFromString(string: String): K

    /**
     * A helpful format of all names and keys necessary to identify a document in this cache.
     *
     * Form: "databaseName.cacheName@key"
     */
    fun getKeyNamespace(key: K): String {
        return "${registration.databaseName}.$cacheName@${keyToString(key)}"
    }

    // ------------------------------------------------------------ //
    //                     Internal Cache Methods                   //
    // ------------------------------------------------------------ //
    @ApiStatus.Internal
    fun cacheInternal(doc: D, log: Boolean = true)

    /**
     * @return If a document was removed from the cache.
     */
    @ApiStatus.Internal
    fun uncacheInternal(doc: D): Boolean

    /**
     * @return If a document was removed from the cache.
     */
    @ApiStatus.Internal
    fun uncacheInternal(key: K): Boolean

    @ApiStatus.Internal
    fun getLoggerInternal(): LoggerService
}
