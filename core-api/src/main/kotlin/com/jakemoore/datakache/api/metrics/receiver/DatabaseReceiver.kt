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
}
