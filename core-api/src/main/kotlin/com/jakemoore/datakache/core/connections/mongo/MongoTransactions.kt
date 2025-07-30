package com.jakemoore.datakache.core.connections.mongo

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.update.TransactionRetriesExceededException
import com.jakemoore.datakache.core.connections.TransactionResult
import com.jakemoore.datakache.core.serialization.util.SerializationUtil
import com.jakemoore.datakache.util.DataKacheFileLogger
import com.mongodb.MongoCommandException
import com.mongodb.client.model.Filters
import com.mongodb.kotlin.client.coroutine.ClientSession
import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoCollection
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.launch
import java.util.Random
import kotlin.coroutines.CoroutineContext

@Suppress("MemberVisibilityCanBePrivate")
object MongoTransactions : CoroutineScope {
    // TODO - future: Add configuration support for these const values
    private const val MAX_TRANSACTION_ATTEMPTS: Int = 50
    private const val LOG_WRITE_CONFLICT_AFTER: Int = 10 // log after 10 attempts
    private const val LOG_WRITE_CONFLICT_FREQUENCY: Int = 5 // log every 5 attempts
    private const val WRITE_CONFLICT_ERROR_CODE = 112 // MongoDB error code for write conflicts

    // Backoff values
    private const val MIN_BACKOFF_MS: Long = 50
    private const val MAX_BACKOFF_MS: Long = 2000
    private const val PING_MULTIPLIER: Double = 2.0 // Base multiplier for ping time
    private const val ATTEMPT_MULTIPLIER: Double = 1.5 // How much to increase per attempt

    /**
     * Execute a MongoDB document update with retries and version filter (CAS logic).
     */
    @Throws(DocumentNotFoundException::class, TransactionRetriesExceededException::class)
    suspend fun <K : Any, D : Doc<K, D>> update(
        client: MongoClient,
        collection: MongoCollection<D>,
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D
    ): D {
        // retryExecutionHelper may throw an exception, which is handled by the caller
        val updatedDoc: D = recursiveRetryUpdate(
            client,
            collection,
            docCache,
            doc,
            updateFunction,
            0
        )

        // Update cache
        docCache.cacheInternal(updatedDoc)
        return updatedDoc
    }

    // If no error is thrown, this method succeeded
    @Throws(DocumentNotFoundException::class, TransactionRetriesExceededException::class)
    private suspend fun <K : Any, D : Doc<K, D>> recursiveRetryUpdate(
        client: MongoClient,
        collection: MongoCollection<D>,
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D,
        attemptNum: Int
    ): D {
        // Check max attempts (recursive base case)
        if (attemptNum >= MAX_TRANSACTION_ATTEMPTS) {
            throw TransactionRetriesExceededException(
                "Failed to execute update after $MAX_TRANSACTION_ATTEMPTS attempts."
            )
        }

        // Apply backoff
        if (attemptNum > 0) {
            delay(calculateBackoffMS(attemptNum))
        }

        // Use a transaction for the update operation to ensure atomicity and consistency
        client.startSession().use { session ->
            session.startTransaction()
            var sessionResolved = false
            try {
                // Fetch Version prior to updates
                val result = getTransactionResult(session, collection, docCache, doc, updateFunction)

                // Receiving a database document indicates failure
                if (result.databaseDoc != null) {
                    sessionResolved = true
                    session.abortTransaction()

                    // Retry with the doc from database (hopefully optimistic versioning works this time)
                    return recursiveRetryUpdate(
                        client,
                        collection,
                        docCache,
                        result.databaseDoc,
                        updateFunction,
                        attemptNum + 1
                    )
                }

                session.commitTransaction()
                sessionResolved = true

                return requireNotNull(result.doc)
            } catch (mE: MongoCommandException) {
                if (mE.errorCode == WRITE_CONFLICT_ERROR_CODE) {
                    logWriteConflict(attemptNum, mE, docCache, doc)

                    // We allow retry-able writes, which sometimes produce write conflicts.
                    // If that occurs, the data isn't stale, we just need to retry the operation.
                    return recursiveRetryUpdate(
                        client,
                        collection,
                        docCache,
                        doc,
                        updateFunction,
                        attemptNum + 1
                    )
                }
                throw mE
            } finally {
                if (!sessionResolved && session.hasActiveTransaction()) {
                    session.abortTransaction()
                    // no log needed, we will throw exceptions to the caller
                }
            }
        }
    }

    private suspend fun <K : Any, D : Doc<K, D>> getTransactionResult(
        session: ClientSession,
        collection: MongoCollection<D>,
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D,
    ): TransactionResult<K, D> {
        val currentVersion: Long = doc.version
        val nextVersion = currentVersion + 1
        val id: String = docCache.keyToString(doc.key)
        val namespace = docCache.getKeyNamespace(doc.key)

        // Apply the Update Function
        val updatedDoc: D = updateFunction(doc).copyHelper(nextVersion)
        require(updatedDoc !== doc) {
            "Update function (for $namespace) must return a new doc (using data class copy)"
        }

        // Validate ID Property
        require(id == docCache.keyToString(updatedDoc.key)) {
            "Updated doc ($namespace) failed key check! Found: ${updatedDoc.key}, Expected: ${doc.key}"
        }

        // Validate Incremented Version (Optimistic Versioning)
        require(updatedDoc.version == nextVersion) {
            "Updated doc ($namespace) failed copy version check! " +
                "Found: ${updatedDoc.version}, Expected: $nextVersion"
        }

        val keyFieldName = SerializationUtil.getSerialNameForKey(docCache)
        val verFieldName = SerializationUtil.getSerialNameForVersion(docCache)
        val result = collection.replaceOne(
            session,
            Filters.and(
                // Filters serve as a compare-and-swap mechanism
                //  to ensure we only update the document if it matches the expected id and version.
                // (Optimistic Versioning)
                Filters.eq(keyFieldName, id),
                Filters.eq(verFieldName, currentVersion),
            ),
            updatedDoc
        )

        // If no documents were modified, then the compare-and-swap failed, we must retry
        if (result.modifiedCount == 0L) {
            DataKache.logger.debug(
                "Failed to update Doc in MongoDB Layer " +
                    "(Could not find document with id: '$namespace' and version: $currentVersion)"
            )

            // If update failed, fetch current version
            val databaseDoc: D = collection.find(session).filter(
                Filters.eq(keyFieldName, id)
            ).firstOrNull() ?: throw RuntimeException(
                "Doc not found for collection: ${docCache.cacheName}, id: $keyFieldName -> $id"
            )

            // Update our working copy with latest version and retry
            return TransactionResult(null, databaseDoc)
        }

        // Success! Return a successful result
        return TransactionResult(updatedDoc, null)
    }

    // Applies linear backoff (with jitter) based on the attempt number and average ping time of the database.
    private val random = Random()
    private fun calculateBackoffMS(attempt: Int): Long {
        // Convert round time into one-way ping time
        val pingNanos = DataKache.storageMode.databaseService.averagePingNanos / 2
        val basePingMs = (pingNanos / 1000000) * PING_MULTIPLIER.toLong()

        // Calculate backoff with attempt scaling
        var backoffMs = basePingMs + (basePingMs * ATTEMPT_MULTIPLIER * attempt).toLong()

        // Clamp the value between min and max
        backoffMs = backoffMs.coerceAtLeast(MIN_BACKOFF_MS).coerceAtMost(MAX_BACKOFF_MS)

        // Add jitter (±25%)
        val half = backoffMs / 4 // 25% of total
        val jitter = random.nextLong(-half, half)

        return backoffMs + jitter
    }

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO

    internal fun <K : Any, D : Doc<K, D>> logWriteConflict(
        currentAttempt: Int,
        mE: MongoCommandException,
        docCache: DocCache<K, D>,
        doc: D,
    ) {
        launch {
            if ((currentAttempt + 1) < LOG_WRITE_CONFLICT_AFTER) return@launch

            val msg = "Write Conflict, attempt " + (currentAttempt + 1) +
                " of " + MAX_TRANSACTION_ATTEMPTS + " for coll " + docCache.cacheName +
                " with id " + docCache.keyToString(doc.key)
            if ((currentAttempt + 1) % LOG_WRITE_CONFLICT_FREQUENCY == 0) {
                DataKacheFileLogger.debug(
                    msg,
                    mE,
                    DataKacheFileLogger.randomWriteExceptionFile
                )
            } else {
                DataKache.logger.info(msg)
            }
        }
    }
}
