package com.jakemoore.datakache.core.connections.mongo

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.exception.doc.InvalidDocCopyHelperException
import com.jakemoore.datakache.api.exception.update.DocumentUpdateException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentKeyModificationException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentVersionModificationException
import com.jakemoore.datakache.api.exception.update.TransactionRetriesExceededException
import com.jakemoore.datakache.api.exception.update.UpdateFunctionReturnedSameInstanceException
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiver
import com.jakemoore.datakache.core.connections.TransactionResult
import com.jakemoore.datakache.core.connections.mongo.MongoDatabaseService.Companion.DUPLICATE_KEY_VIOLATION_CODE
import com.jakemoore.datakache.core.serialization.util.SerializationUtil
import com.jakemoore.datakache.util.DataKacheFileLogger
import com.mongodb.MongoCommandException
import com.mongodb.MongoWriteException
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
    @Throws(DocumentNotFoundException::class, DuplicateUniqueIndexException::class, TransactionRetriesExceededException::class)
    suspend fun <K : Any, D : Doc<K, D>> update(
        client: MongoClient,
        collection: MongoCollection<D>,
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D
    ): D {
        val startMS = System.currentTimeMillis()

        // retryExecutionHelper may throw an exception, which is handled by the caller
        val updatedDoc: D = recursiveRetryUpdate(
            client,
            collection,
            docCache,
            doc,
            updateFunction,
            0
        )

        val elapsedMS = System.currentTimeMillis() - startMS
        // METRICS
        DataKacheMetrics.receivers.forEach { it.onDatabaseUpdateTransactionSuccess(elapsedMS) }

        // Update cache
        docCache.cacheInternal(updatedDoc)
        return updatedDoc
    }

    // If no error is thrown, this method succeeded
    @Throws(DocumentNotFoundException::class, DuplicateUniqueIndexException::class, TransactionRetriesExceededException::class)
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
            // METRICS
            DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseUpdateTransactionLimitReached)

            throw TransactionRetriesExceededException(
                "Failed to execute update after $MAX_TRANSACTION_ATTEMPTS attempts."
            )
        }

        // Apply backoff
        if (attemptNum > 0) {
            delay(calculateBackoffMS(attemptNum))
        }

        // METRICS
        val startMS = System.currentTimeMillis()
        DataKacheMetrics.receivers.forEach(MetricsReceiver::onDatabaseUpdateTransactionAttemptStart)

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

                    // METRICS
                    logAttemptTimeMetric(startMS)

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

                // METRICS
                logAttemptTimeMetric(startMS)
                DataKacheMetrics.receivers.forEach {
                    it.onDatabaseUpdateTransactionAttemptsRequired(attemptNum + 1)
                }

                return requireNotNull(result.doc)
            } catch (mE: MongoCommandException) {
                // write conflict may occur and is not to worry, we can retry
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
            } catch (e: MongoWriteException) {
                // Handle duplicate unique index exceptions
                if (e.error.code == DUPLICATE_KEY_VIOLATION_CODE) {
                    val errorMessage = e.message ?: ""
                    when {
                        errorMessage.contains("index: _id") -> {
                            // Primary key violation (duplicate _id)
                            throw DuplicateDocumentKeyException(
                                docCache = docCache,
                                docCache.keyToString(doc.key),
                                fullMessage = errorMessage,
                                operation = "update",
                            )
                        }
                        errorMessage.contains("index:") -> {
                            // Unique index violation (duplicate value in a unique index)
                            throw DuplicateUniqueIndexException(
                                docCache = docCache,
                                fullMessage = errorMessage,
                                operation = "update",
                            )
                        }
                    }
                }
                throw e
            } catch (e: DocumentNotFoundException) {
                // promote exception to caller
                throw e
            } catch (e: DocumentUpdateException) {
                // promote exception to caller
                throw e
            } catch (e: InvalidDocCopyHelperException) {
                // promote exception to caller
                throw e
            } finally {
                if (!sessionResolved && session.hasActiveTransaction()) {
                    session.abortTransaction()
                    // no log needed, we will throw exceptions to the caller
                }
            }
        }
    }

    @Throws(DocumentNotFoundException::class, DocumentUpdateException::class, InvalidDocCopyHelperException::class)
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
        val rawUpdated: D = updateFunction(doc)
        if (rawUpdated === doc) {
            throw UpdateFunctionReturnedSameInstanceException(namespace)
        }

        // Validate ID Property
        val foundKeyString = docCache.keyToString(rawUpdated.key)
        if (id != foundKeyString) {
            throw IllegalDocumentKeyModificationException(namespace, foundKeyString, id)
        }

        // Validate Incremented Version (Optimistic Versioning)
        val foundVersion = rawUpdated.version
        if (currentVersion != foundVersion) {
            throw IllegalDocumentVersionModificationException(namespace, foundVersion, nextVersion)
        }

        // Increment the version on the validated document
        val updatedDoc = rawUpdated.copyHelper(nextVersion)
        if (updatedDoc.version != nextVersion) {
            throw InvalidDocCopyHelperException(
                docNamespace = namespace,
                message = "The copyHelper did not return a document with the expected version. " +
                    "Expected version: $nextVersion, but got: ${updatedDoc.version}."
            )
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
            ).firstOrNull() ?: run {
                throw DocumentNotFoundException(
                    keyString = id,
                    docCache = docCache,
                    operation = "update",
                )
            }

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

    private fun logAttemptTimeMetric(startMS: Long) {
        val elapsedMS = System.currentTimeMillis() - startMS
        DataKacheMetrics.receivers.forEach { it.onDatabaseUpdateTransactionAttemptTime(elapsedMS) }
    }
}
