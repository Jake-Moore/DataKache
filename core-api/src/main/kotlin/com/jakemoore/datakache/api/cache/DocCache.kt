package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.api.cache.config.DocCacheConfig
import com.jakemoore.datakache.api.coroutines.DataKacheScope
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.doc.Doc.Status
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.exception.update.DocumentUpdateException
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.exception.update.TransactionRetriesExceededException
import com.jakemoore.datakache.api.exception.update.UpdateQueueShutdownException
import com.jakemoore.datakache.api.exception.update.UpdateQueueStalledException
import com.jakemoore.datakache.api.exception.update.UpdateQueueTooDeepException
import com.jakemoore.datakache.api.index.DocUniqueIndex
import com.jakemoore.datakache.api.java.ThrowingUnaryOperator
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.metrics.ChangeStreamQueueStats
import com.jakemoore.datakache.api.ordering.OperationTime
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.api.result.RejectableResult
import com.jakemoore.datakache.api.result.Success
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.future
import kotlinx.serialization.KSerializer
import org.jetbrains.annotations.ApiStatus
import org.jetbrains.annotations.NonBlocking
import java.util.concurrent.CompletableFuture
import java.util.function.UnaryOperator
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
    fun getStatus(doc: D): Status = getStatus(doc.key, doc.version)

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
     * - [UpdateQueueStalledException]: The queue serialising updates for this document stopped
     *   making progress.
     * - [UpdateQueueTooDeepException]: The queue kept progressing but this update never reached the
     *   front of it.
     * - [UpdateQueueShutdownException]: The queue was shut down before this update finished.
     *
     * **The last three mean the outcome is unknown rather than failed.** Only the waiting was given
     * up; the queue still owns the update and may complete it afterwards. Retrying an update that
     * is not idempotent after one of those can apply it twice.
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
     * - [UpdateQueueStalledException]: The queue serialising updates for this document stopped
     *   making progress.
     * - [UpdateQueueTooDeepException]: The queue kept progressing but this update never reached the
     *   front of it.
     * - [UpdateQueueShutdownException]: The queue was shut down before this update finished.
     *
     * **The last three mean the outcome is unknown rather than failed.** Only the waiting was given
     * up; the queue still owns the update and may complete it afterwards. Retrying an update that
     * is not idempotent after one of those can apply it twice.
     */
    suspend fun update(doc: D, updateFunction: (D) -> D): DefiniteResult<D> = update(doc.key, updateFunction)

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
    suspend fun updateRejectable(doc: D, updateFunction: (D) -> D): RejectableResult<D> =
        updateRejectable(doc.key, updateFunction)

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
    suspend fun delete(doc: D): DefiniteResult<Boolean> = delete(doc.key)

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
     * REQUIRES [DocCacheConfig.enableMassDestructiveOps] to be true, otherwise it will throw [IllegalStateException]
     *
     * @return A [DefiniteResult] indicating success or failure of the operation.
     * (long value indicates number of documents deleted)
     */
    suspend fun clearDocsFromDatabasePermanently(): DefiniteResult<Long>

    // ------------------------------------------------------------ //
    //                     CRUD Database Methods                    //
    // ------------------------------------------------------------ //

    /**
     * Fetch a document from the **database** (skipping cache).
     *
     * The result populates the cache only if [key] has never been cached or deleted before. A read
     * cannot safely refresh a key that has, because it carries no position in the database's own
     * ordering and a slow read could otherwise overwrite state a concurrent write already made
     * newer. Use [read] first if the caller only wants to know what is currently cached.
     *
     * @param key The unique key of the document to be fetched.
     *
     * @return An [OptionalResult] containing the document if it exists, or empty if it does not.
     */
    suspend fun readFromDatabase(key: K): OptionalResult<D>

    /**
     * Fetch all documents from the **database** (skipping cache) as a [Flow].
     *
     * Each document populates the cache under the same rule as [readFromDatabase]: only a key with
     * no existing position is written back.
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
    fun getKeyNamespace(key: K): String = "${registration.databaseName}.$cacheName@${keyToString(key)}"

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
    suspend fun <T> registerUniqueIndex(index: DocUniqueIndex<K, D, T>): DefiniteResult<Unit>

    /**
     * Attempts to read a document from the cache by a unique index. (ONLY checks cache)
     *
     * @param index The unique index previously registered on this cache.
     * @param value The value in the index to search for.
     *
     * @return The [OptionalResult] containing the document if found, or empty if it does not.
     */
    fun <T> readByUniqueIndex(index: DocUniqueIndex<K, D, T>, value: T): OptionalResult<D>

    /**
     * Attempts to read a document from the **database** by a unique index. (ONLY checks database)
     *
     * @param index The unique index previously registered on this cache.
     * @param value The value in the index to search for.
     *
     * @return The [OptionalResult] containing the document if found, or empty if it does not.
     */
    suspend fun <T> readByUniqueIndexFromDatabase(index: DocUniqueIndex<K, D, T>, value: T): OptionalResult<D>

    /**
     * A snapshot of this cache's change stream event buffer, or null if the stream is not running.
     *
     * Intended for exporting as a gauge. Events are applied from a bounded buffer in the database's
     * commit order, and a full buffer pauses the stream rather than dropping or reordering anything,
     * so depth approaching [ChangeStreamQueueStats.capacity] means the cache is falling behind the
     * database. **Reading resets [ChangeStreamQueueStats.peakSinceLastRead]**, so poll it from one
     * place.
     */
    fun getChangeStreamQueueStats(): ChangeStreamQueueStats?

    // ------------------------------------------------------------ //
    //                     Internal Cache Methods                   //
    // ------------------------------------------------------------ //

    /**
     * Applies [doc] to the cache at position [at] in the database's ordering, if [at] is newer than
     * the position the cache already holds for [doc]'s key.
     *
     * [DocCacheConfig.optimisticCaching] applies only when the caller passes [isReplayedEvent] true,
     * and that is safe to do only where "same version" is trustworthy evidence of "same content" --
     * where the operation that produced [doc] GUARANTEES its version differs from whatever was there
     * before whenever the content does. Local writes must never pass it, regardless of operation
     * type: they are authoritative on their own content, and have nothing to gain by risking a skip.
     *
     * For the three change-stream replay sites, per operation type:
     * - UPDATE: [isReplayedEvent] true. [updateInternal]'s transaction enforces the guarantee
     *   unconditionally, via `copyHelper(nextVersion)` on every attempt including retries, so a
     *   version match really does mean this exact update was already applied.
     * - REPLACE: [isReplayedEvent] false, unconditionally. A database replace carries no such
     *   guarantee -- it can legitimately keep a document's existing version, as a reset rather than
     *   an increment, in which case "same version" says nothing about content.
     * - INSERT: [isReplayedEvent] false too, though for a different reason: on the ordinary path the
     *   key had no prior document, so `cached` is null and the flag could never matter. Passing true
     *   would buy nothing there and would reopen the REPLACE class of bug on the one path where
     *   `cached` can be non-null: a delete whose own cache removal was skipped, racing a recreate
     *   that reuses the same starting version.
     *
     * The stakes for getting this wrong are not merely a stale read. Local writes and their own
     * change-stream replay carry the IDENTICAL operation time, since it is one write viewed from two
     * places, so whichever of them reaches this method first decides the position for both. A skip
     * still advances the position ([DocCacheImpl]'s `appliedAt`), so if the winner of that race skips
     * its own write, the position is claimed with nothing behind it, and the loser -- carrying that
     * same operation time -- is then refused as not strictly newer. The content is never applied by
     * either side. Applying unconditionally is what keeps this safe regardless of which side wins.
     */
    @ApiStatus.Internal
    fun cacheInternal(doc: D, at: OperationTime, log: Boolean = true, isReplayedEvent: Boolean = false)

    /**
     * @return If a document was removed from the cache.
     */
    @ApiStatus.Internal
    fun uncacheInternal(doc: D, at: OperationTime): Boolean

    /**
     * @return If a document was removed from the cache.
     */
    @ApiStatus.Internal
    fun uncacheInternal(key: K, at: OperationTime): Boolean

    /**
     * Caches a document read back from the database without claiming a position in the ordering.
     *
     * A read reflects committed state, so its content is safe to cache, but the read itself carries
     * no operation time here. Advancing the ordering with a guess would let a legitimate later event
     * be refused, so a read updates content and leaves the ordering alone.
     */
    @ApiStatus.Internal
    fun cacheContentOnlyInternal(doc: D, log: Boolean = true)

    @ApiStatus.Internal
    fun getLoggerInternal(): LoggerService

    /**
     * Throws an exception if the update is not valid according to the cache's rules.
     *
     * If this method returns successfully, the update is valid and can be applied.
     *
     * @param originalDoc The original document before the update.
     * @param updatedDoc The document after the update.
     *
     * @throws DocumentUpdateException if the update breaks a document update rule.
     */
    @ApiStatus.Internal
    @Throws(DocumentUpdateException::class)
    fun isUpdateValidInternal(originalDoc: D, updatedDoc: D)

    /**
     * Checks if the change stream jobs are currently running.
     */
    @ApiStatus.Internal
    fun areChangeStreamJobsRunning(): Boolean

    // ------------------------------------------------------------ //
    //              Java Compatibility — CompletableFuture API      //
    // ------------------------------------------------------------ //

    fun createAsync(key: K): CompletableFuture<DefiniteResult<D>> = future { create(key) }

    fun createAsync(key: K, initializer: UnaryOperator<D>): CompletableFuture<DefiniteResult<D>> =
        future { create(key) { initializer.apply(it) } }

    fun readOrCreateAsync(key: K): CompletableFuture<DefiniteResult<D>> = future { readOrCreate(key) }

    fun readOrCreateAsync(key: K, initializer: UnaryOperator<D>): CompletableFuture<DefiniteResult<D>> =
        future { readOrCreate(key) { initializer.apply(it) } }

    fun updateAsync(key: K, updateFunction: UnaryOperator<D>): CompletableFuture<DefiniteResult<D>> =
        future { update(key) { updateFunction.apply(it) } }

    fun updateAsync(doc: D, updateFunction: UnaryOperator<D>): CompletableFuture<DefiniteResult<D>> =
        future { update(doc) { updateFunction.apply(it) } }

    fun updateRejectableAsync(
        key: K,
        updateFunction: ThrowingUnaryOperator<D>,
    ): CompletableFuture<RejectableResult<D>> = future { updateRejectable(key) { updateFunction.apply(it) } }

    fun updateRejectableAsync(
        doc: D,
        updateFunction: ThrowingUnaryOperator<D>,
    ): CompletableFuture<RejectableResult<D>> = future { updateRejectable(doc) { updateFunction.apply(it) } }

    fun deleteAsync(key: K): CompletableFuture<DefiniteResult<Boolean>> = future { delete(key) }

    fun deleteAsync(doc: D): CompletableFuture<DefiniteResult<Boolean>> = future { delete(doc) }

    fun clearDocsFromDatabasePermanentlyAsync(): CompletableFuture<DefiniteResult<Long>> =
        future { clearDocsFromDatabasePermanently() }

    fun readFromDatabaseAsync(key: K): CompletableFuture<OptionalResult<D>> = future { readFromDatabase(key) }

    fun readSizeFromDatabaseAsync(): CompletableFuture<DefiniteResult<Long>> = future { readSizeFromDatabase() }

    fun hasKeyInDatabaseAsync(key: K): CompletableFuture<DefiniteResult<Boolean>> = future { hasKeyInDatabase(key) }

    /**
     * Java-compatible variant of [readAllFromDatabase].
     *
     * Collects the [Flow] into a [List] before resolving. For large collections, consider
     * using the Kotlin [readAllFromDatabase] Flow-returning variant directly.
     */
    fun readAllFromDatabaseAsync(): CompletableFuture<DefiniteResult<List<D>>> =
        future {
        @Suppress("UNCHECKED_CAST")
        when (val r = readAllFromDatabase()) {
            is Success -> Success(r.getOrThrow().toList())
            is Failure -> r as DefiniteResult<List<D>>
        }
    }

    /**
     * Java-compatible variant of [readKeysFromDatabase].
     *
     * Collects the [Flow] into a [List] before resolving.
     */
    fun readKeysFromDatabaseAsync(): CompletableFuture<DefiniteResult<List<K>>> =
        future {
        @Suppress("UNCHECKED_CAST")
        when (val r = readKeysFromDatabase()) {
            is Success -> Success(r.getOrThrow().toList())
            is Failure -> r as DefiniteResult<List<K>>
        }
    }

    fun <T> registerUniqueIndexAsync(index: DocUniqueIndex<K, D, T>): CompletableFuture<DefiniteResult<Unit>> =
        future { registerUniqueIndex(index) }
}
