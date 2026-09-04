package com.jakemoore.datakache.api.metrics.receiver

/**
 * Metrics receiver for database operations, failures, and transaction events.
 */
interface DatabaseReceiver {
    // CRUD Operations
    fun onDatabaseInsert()

    fun onDatabaseUpdate()

    fun onDatabaseRead()

    fun onDatabaseDelete()

    fun onDatabaseReadAll()

    fun onDatabaseSize()

    fun onDatabaseHasKey()

    fun onDatabaseClear()

    fun onDatabaseReadKeys()

    fun onDatabaseReplace()

    // Fail States
    fun onDatabaseInsertFail()

    fun onDatabaseUpdateFail()

    fun onDatabaseUpdateDocNotFoundFail()

    fun onDatabaseReadFail()

    fun onDatabaseDeleteFail()

    fun onDatabaseReadAllFail()

    fun onDatabaseSizeFail()

    fun onDatabaseHasKeyFail()

    fun onDatabaseClearFail()

    fun onDatabaseReadKeysFail()

    fun onDatabaseReplaceFail()

    // Update Transaction Events
    fun onDatabaseUpdateTransactionLimitReached()

    fun onDatabaseUpdateTransactionAttemptStart()

    fun onDatabaseUpdateTransactionAttemptTime(milliseconds: Long)

    /**
     * How many attempts were required for the transaction to succeed
     */
    fun onDatabaseUpdateTransactionAttemptsRequired(attempts: Int)

    fun onDatabaseUpdateTransactionSuccess(milliseconds: Long)

    // Update Queue Health

    /**
     * The queue serialising updates for one document stopped making progress, or never reached a
     * caller within the ceiling.
     *
     * Both are faults rather than load: a busy queue does not raise this however deep it gets. Worth
     * alerting on rather than graphing, since a healthy deployment never emits it.
     *
     * Has a default body, unlike the rest of this interface, so that adding it does not break
     * existing implementers.
     */
    fun onUpdateQueueStalled(cacheName: String, docKeyString: String, queueDepth: Long) {}

    /**
     * A document is being written faster than it can be written: the queue kept completing, but a
     * caller's turn never arrived within the ceiling.
     *
     * Separate from [onUpdateQueueStalled] because the remedy is different. That one means
     * something is wedged; this one means the queue is healthy and the write rate is not.
     *
     * [waitedMs] is carried because the ceiling it crossed is a deployment constant, so it is the
     * only value here that says how far past the ceiling this caller got.
     *
     * Has a default body for the same reason as above.
     */
    fun onUpdateQueueTooDeep(cacheName: String, docKeyString: String, waitedMs: Long, queueDepth: Long) {}
}
