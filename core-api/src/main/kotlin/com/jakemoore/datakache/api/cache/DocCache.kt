package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.api.cache.config.DocCacheConfig
import com.jakemoore.datakache.api.coroutines.DataKacheScope
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.doc.Doc.Status
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.exception.update.TransactionRetriesExceededException
import com.jakemoore.datakache.api.index.DocUniqueIndex
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.api.result.RejectableResult
import com.jakemoore.datakache.api.result.Success
import kotlinx.coroutines.flow.Flow
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
    //                          API Methods                         //
    // ------------------------------------------------------------ //
    /**
     * Returns the status of a document based on its version and the current state of the cache.
     * Possible Status Values:
     * - [Status.FRESH]: Cache contains the exact same document and version. Data is up-to-date.
     * - [Status.STALE]: Cache contains a different version of the document. Data is outdated.
     * - [Status.DELETED]: Cache does not contain the document at all. Data is considered deleted.
     */
    fun getStatus(key: K, version: Long): Status

    /**
     * Returns the status of a document based on its version and the current state of the cache.
     * Possible Status Values:
     * - [Status.FRESH]: Cache contains the exact same document and version. Data is up-to-date.
     * - [Status.STALE]: Cache contains a different version of the document. Data is outdated.
     * - [Status.DELETED]: Cache does not contain the document at all. Data is considered deleted.
     */
    fun getStatus(doc: D): Status {
        return getStatus(doc.key, doc.version)
    }

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
     * Creates a new document in the cache (backed by a database object).
     *
     * @param key The unique key for the document to be created.
     * @param initializer A callback function for initializing the document with starter data.
     *
     * @return A [DefiniteResult] containing the document, or the exception if the document could not be created.
     */
    suspend fun create(key: K, initializer: (D) -> D = { it }): DefiniteResult<D>

    /**
     * Fetches (or creates) a document in the cache by its key. Due to the nature of this event (creative),
     * it may require database calls and therefore may not be instantaneous.
     *
     * Failures from reading will be passed through via [DefiniteResult].
     *
     * @param key The unique key for the document to be read or created.
     * @param initializer A callback function for initializing the document with starter data (when it does not exist)
     *
     * @return A [DefiniteResult] containing the document, or the exception if the document could not be found/created.
     */
    suspend fun readOrCreate(key: K, initializer: (D) -> D = { it }): DefiniteResult<D> {
        // While DataKache intends to keep all documents in cache, we are already in a suspend context
        //  so I believe it is acceptable to perform a **database** read here instead of a cache check.
        // Our result will be more certain, which is necessary since a miss will try to create the document.
        //  and this helps minimize the chance of a DuplicateDocumentKeyException.
        return when (val result = readFromDatabase(key)) {
            is Success, is Failure -> {
                // If we found the document, return it
                // Likewise, if we encountered a failure exception, pass it through
                result
            }
            is Empty -> {
                // Time to create the document
                create(key, initializer)
            }
        }
    }

    /**
     * Modify a document by its key (both cache and database will be updated).
     *
     * @param key The unique key of the document to be updated.
     *
     * Returns A [DefiniteResult] containing the updated document, or an exception if the document could not be updated.
     * If [DefiniteResult] is a [Failure], common exceptions include:
     * - [DocumentNotFoundException]: The document with the given key does not exist in the cache or database.
     * - [DuplicateUniqueIndexException]: The update operation violates a unique index constraint.
     * - [TransactionRetriesExceededException]: The update operation failed after exceeding allowed transaction retries.
     */
    suspend fun update(key: K, updateFunction: (D) -> D): DefiniteResult<D>

    /**
     * Modify a document by its key (both cache and database will be updated).
     *
     * @param doc The document to be updated (will be updated via its key).
     *
     * Returns A [DefiniteResult] containing the updated document, or an exception if the document could not be updated.
     * If [DefiniteResult] is a [Failure], common exceptions include:
     * - [DocumentNotFoundException]: The document with the given key does not exist in the cache or database.
     * - [DuplicateUniqueIndexException]: The update operation violates a unique index constraint.
     * - [TransactionRetriesExceededException]: The update operation failed after exceeding allowed transaction retries.
     */
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
     * See parent implementations for details on the behavior of this method:
     * - [GenericDocCache.delete]
     */
    suspend fun delete(key: K): DefiniteResult<Boolean>

    /**
     * Alias of [DocCache.delete], passing the document's key.
     *
     * See [DocCache.delete] for more information.
     */
    suspend fun delete(doc: D): DefiniteResult<Boolean> {
        return delete(doc.key)
    }

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

    /**
     * Fetch the total size of all documents in the cache.
     */
    fun getCacheSize(): Int

    /**
     * Clears ALL documents from the cache AND **database**.
     *
     * DATA IS NOT RECOVERABLE AFTER THIS OPERATION.
     *
     * This method is intended for use in testing or when you want to completely reset the cache and database.
     *
     * @return A [DefiniteResult] indicating success or failure of the operation.
     * (long value indicates number of documents deleted)
     */
    suspend fun clearAllPermanently(): DefiniteResult<Long>

    // ------------------------------------------------------------ //
    //                     CRUD Database Methods                    //
    // ------------------------------------------------------------ //
    /**
     * Fetch a document from the **database** (skipping cache).
     *
     * This document will be automatically cached on success.
     *
     * @param key The unique key of the document to be fetched.
     *
     * @return An [OptionalResult] containing the document if it exists, or empty if it does not.
     */
    suspend fun readFromDatabase(key: K): OptionalResult<D>

    /**
     * Fetch all documents from the **database** (skipping cache) as a [Flow].
     *
     * All documents will be automatically cached on success.
     *
     * @return An [DefiniteResult] containing a [Flow] of documents.
     */
    suspend fun readAllFromDatabase(): DefiniteResult<Flow<D>>

    /**
     * Counts the total number of documents in the **database**.
     *
     * This does not check the cache, only the database.
     *
     * @return The total number of documents in the database.
     */
    suspend fun readSizeFromDatabase(): DefiniteResult<Long>

    /**
     * Checks if the **database** has a document with the given key.
     *
     * This does not check the cache, only the database.
     *
     * @param key The unique key of the document to check.
     *
     * @return True if the document exists in the database, false otherwise.
     */
    suspend fun hasKeyInDatabase(key: K): DefiniteResult<Boolean>

    /**
     * Fetch all document keys from the **database** (skipping cache) as a [Flow].
     *
     * @return An [DefiniteResult] containing a [Flow] of document keys.
     */
    suspend fun readKeysFromDatabase(): DefiniteResult<Flow<K>>

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

    /**
     * Configuration options for this cache.
     *
     * See the cache constructor in order to provide your own configuration object.
     */
    val config: DocCacheConfig<K, D>

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
    //                         Unique Indexes                       //
    // ------------------------------------------------------------ //
    /**
     * Register a custom index for this cache.
     *
     * This index uses one of your custom data properties as the backing field.
     *
     * This index will have uniqueness constraints enforced, similar to a superkey.
     *
     * @return A [DefiniteResult] indicating success or failure of the registration.
     */
    suspend fun <T> registerUniqueIndex(
        index: DocUniqueIndex<K, D, T>,
    ): DefiniteResult<Unit>

    /**
     * Attempts to read a document from the cache by a unique index. (ONLY checks cache)
     *
     * @param index The unique index previously registered on this cache.
     * @param value The value in the index to search for.
     *
     * @return The [OptionalResult] containing the document if found, or empty if it does not.
     */
    fun <T> readByUniqueIndex(
        index: DocUniqueIndex<K, D, T>,
        value: T,
    ): OptionalResult<D>

    /**
     * Attempts to read a document from the **database** by a unique index. (ONLY checks database)
     *
     * @param index The unique index previously registered on this cache.
     * @param value The value in the index to search for.
     *
     * @return The [OptionalResult] containing the document if found, or empty if it does not.
     */
    suspend fun <T> readByUniqueIndexFromDatabase(
        index: DocUniqueIndex<K, D, T>,
        value: T,
    ): OptionalResult<D>

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
