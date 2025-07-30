package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.api.coroutines.DataKacheScope
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.api.result.RejectableResult
import com.jakemoore.datakache.core.Service
import kotlinx.serialization.KSerializer
import org.jetbrains.annotations.NonBlocking
import kotlin.reflect.KProperty

@Suppress("unused")
interface DocCache<K : Any, D : Doc<K, D>> : Service, DataKacheScope {
    // ------------------------------------------------------------ //
    //                     Kotlin Reflect Access                    //
    // ------------------------------------------------------------ //
    fun getKSerializer(): KSerializer<D>
    fun getKeyKProperty(): KProperty<K>
    fun getVersionKProperty(): KProperty<Long>

    // ----------------------------------------------------- //
    //                     CRUD Methods                      //
    // ----------------------------------------------------- //
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
    suspend fun update(key: K, updateFunction: (D) -> D): DefiniteResult<D>

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
    suspend fun updateRejectable(key: K, updateFunction: (D) -> D): RejectableResult<D>

    /**
     * Deletes a document from the cache and the backing database.
     *
     * @param key The unique key of the document to be deleted.
     *
     * @return A [DefiniteResult] indicating the outcome of the deletion operation (success=true, failure=false).
     */
    suspend fun delete(key: K): DefiniteResult<Boolean>
}
