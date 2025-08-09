package com.jakemoore.datakache.core.connections.mongo

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.exception.data.Operation
import com.jakemoore.datakache.api.exception.doc.InvalidDocCopyHelperException
import com.jakemoore.datakache.api.exception.update.DocumentUpdateException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentKeyModificationException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentVersionModificationException
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
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
import kotlin.coroutines.CoroutineContext
import kotlin.math.pow
import kotlin.random.Random

@Suppress("MemberVisibilityCanBePrivate")
object MongoTransactions : CoroutineScope {
    // TODO - future: Add configuration support for these const values
    private const val MAX_TRANSACTION_ATTEMPTS: Int = 50
    private const val LOG_WRITE_CONFLICT_AFTER: Int = 10 // log after 10 attempts
    private const val LOG_WRITE_CONFLICT_FREQUENCY: Int = 5 // log every 5 attempts
    private const val WRITE_CONFLICT_ERROR_CODE = 112 // MongoDB error code for write conflicts

    // Backoff values
    private const val MIN_BACKOFF_MS: Long = 5
    private const val MAX_BACKOFF_MS: Long = 5000

    /**
     * Execute a MongoDB document update with retries and version filter (CAS logic).
     *
     * NOTE: This method is now primarily called through the UpdateQueue system,
     * which eliminates most version conflicts by serializing updates to the same document.
     * Retries here are mainly for handling network issues or MongoDB internal conflicts.
     */
    @Throws(
        DocumentNotFoundException::class,
        DuplicateDocumentKeyException::class, DuplicateUniqueIndexException::class,
        TransactionRetriesExceededException::class, DocumentUpdateException::class,
        InvalidDocCopyHelperException::class, UpdateFunctionReturnedSameInstanceException::class,
        IllegalDocumentKeyModificationException::class, IllegalDocumentVersionModificationException::class,
        RejectUpdateException::class,
    )
    suspend fun <K : Any, D : Doc<K, D>> update(
        client: MongoClient,
        collection: MongoCollection<D>,
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D,
        bypassValidation: Boolean,
    ): D {
        val startMS = System.currentTimeMillis()

        // retryExecutionHelper may throw an exception, which is handled by the caller
        val updatedDoc: D = recursiveRetryUpdate(
            client,
            collection,
            docCache,
            doc,
            updateFunction,
            0,
            bypassValidation,
        )

        val elapsedMS = System.currentTimeMillis() - startMS
        // METRICS
        DataKacheMetrics.receivers.forEach { it.onDatabaseUpdateTransactionSuccess(elapsedMS) }

        // Update cache
        docCache.cacheInternal(updatedDoc)
        return updatedDoc
    }

    // If no error is thrown, this method succeeded
    @Throws(
        DocumentNotFoundException::class,
        DuplicateDocumentKeyException::class, DuplicateUniqueIndexException::class,
        TransactionRetriesExceededException::class, DocumentUpdateException::class,
        InvalidDocCopyHelperException::class, UpdateFunctionReturnedSameInstanceException::class,
        IllegalDocumentKeyModificationException::class, IllegalDocumentVersionModificationException::class,
        RejectUpdateException::class,
    )
    private suspend fun <K : Any, D : Doc<K, D>> recursiveRetryUpdate(
        client: MongoClient,
        collection: MongoCollection<D>,
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D,
        attemptNum: Int,
        bypassValidation: Boolean,
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
                val result = getTransactionResult(
                    session,
                    collection,
                    docCache,
                    doc,
                    updateFunction,
                    bypassValidation
                )

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
                        attemptNum + 1,
                        bypassValidation,
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
                        attemptNum + 1,
                        bypassValidation,
                    )
                }
                throw mE
            } catch (e: MongoWriteException) {
                // Handle duplicate unique index exceptions
                checkDuplicateKeyExceptions(e, docCache, doc, Operation.UPDATE)
                throw e
            } finally {
                if (!sessionResolved && session.hasActiveTransaction()) {
                    session.abortTransaction()
                    // no log needed, we will throw exceptions to the caller
                }
            }
        }
    }

    @Throws(
        DuplicateDocumentKeyException::class,
        DuplicateUniqueIndexException::class,
    )
    internal fun <K : Any, D : Doc<K, D>> checkDuplicateKeyExceptions(
        e: MongoWriteException,
        docCache: DocCache<K, D>,
        doc: D,
        operation: Operation,
    ) {
        // We only handle duplicate key violations here
        if (e.error.code != DUPLICATE_KEY_VIOLATION_CODE) {
            return
        }

        val errorMessage = e.message ?: ""
        val index = extractIndexNameFromError(errorMessage)?.lowercase() ?: run {
            DataKacheFileLogger.warn(
                "Failed to extract index name from MongoDB error message: '$errorMessage'. " +
                    "This may indicate a new or unexpected error format. Please report this issue.",
                e,
            )
            return
        }

        when {
            index == "_id" || index == "_id_" -> {
                // Primary key violation (duplicate _id)
                throw DuplicateDocumentKeyException(
                    docCache = docCache,
                    docCache.keyToString(doc.key),
                    fullMessage = errorMessage,
                    operation = operation,
                    cause = e,
                )
            }
            else -> {
                // Unique index violation (duplicate value in a unique index)
                throw DuplicateUniqueIndexException(
                    docCache = docCache,
                    fullMessage = errorMessage,
                    operation = operation,
                    index = index,
                    cause = e,
                )
            }
        }
    }

    @Suppress("RegExpSimplifiable", "RegExpRedundantEscape")
    private fun extractIndexNameFromError(errorMessage: String): String? {
        // More robust regex patterns for different error message formats
        val patterns = listOf(
            Regex("""index:\s*([^\s]+)"""), // Standard format
            Regex("""index\s+"([^"]+)""""), // Quoted index names
            Regex("""dup key:\s*\{\s*:\s*([^}]+)\}""") // Alternative format
        )

        for (pattern in patterns) {
            pattern.find(errorMessage)?.let { match ->
                return match.groupValues[1].trim()
            }
        }
        return null
    }

    @Throws(
        DocumentNotFoundException::class,
        DocumentUpdateException::class,
        InvalidDocCopyHelperException::class,
        UpdateFunctionReturnedSameInstanceException::class,
        IllegalDocumentKeyModificationException::class,
        IllegalDocumentVersionModificationException::class,
        RejectUpdateException::class,
    )
    private suspend fun <K : Any, D : Doc<K, D>> getTransactionResult(
        session: ClientSession,
        collection: MongoCollection<D>,
        docCache: DocCache<K, D>,
        originalDoc: D,
        updateFunction: (D) -> D,
        bypassValidation: Boolean,
    ): TransactionResult<K, D> {
        val currentVersion: Long = originalDoc.version
        val nextVersion = currentVersion + 1
        val id: String = docCache.keyToString(originalDoc.key)
        val namespace = docCache.getKeyNamespace(originalDoc.key)

        // Apply the Update Function
        val rawUpdated: D = updateFunction(originalDoc)
        if (rawUpdated === originalDoc) {
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

        if (!bypassValidation) {
            // Validate additional properties
            // (throws DocumentUpdateException if validation fails)
            docCache.isUpdateValidInternal(originalDoc, updatedDoc)
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
                    operation = Operation.UPDATE,
                )
            }

            // Update our working copy with latest version and retry
            return TransactionResult(null, databaseDoc)
        }

        // Success! Return a successful result
        return TransactionResult(updatedDoc, null)
    }

    // Applies optimized backoff for the queue-based system where conflicts should be rare
    private fun calculateBackoffMS(attempt: Int): Long {
        // Since we're using per-document queues, conflicts should be extremely rare
        // Most retries will be due to network issues or MongoDB internal conflicts
        // Use much shorter delays since version conflicts are eliminated by queuing
        if (attempt <= 2) {
            // For the first few attempts, use minimal backoff (likely network hiccups)
            val jitter = Random.nextLong(10, 31) // 10-30ms jitter
            return MIN_BACKOFF_MS + jitter
        }

        // For subsequent attempts, use a more conservative approach
        // but still much less aggressive than the original algorithm
        val pingNanos = DataKache.storageMode.databaseService.averagePingNanos / 2
        val basePingMs = (pingNanos / 1_000_000).coerceAtLeast(MIN_BACKOFF_MS)

        // Use exponential backoff but with much smaller multiplier
        val backoffMs = basePingMs * (1.2).pow(attempt - 2).toLong()

        // Add jitter (±20%)
        val jitter = Random.nextLong(-backoffMs / 5, backoffMs / 5)

        return (backoffMs + jitter).coerceIn(MIN_BACKOFF_MS, MAX_BACKOFF_MS)
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
