package com.jakemoore.datakache.api.metrics.receiver

/**
 * Metrics receiver for database operations, failures, and transaction events.
 */
interface UniqueIndexReceiver {
    // Read Operations
    fun onDatabaseReadDocByUniqueIndex()
    fun onCacheReadDocByUniqueIndex()
    fun onRegisterUniqueIndex()

    // Read Operation Failures
    fun onDatabaseReadDocByUniqueIndexFail()
    fun onCacheReadDocByUniqueIndexFail()
    fun onRegisterUniqueIndexFail()
}
