package com.jakemoore.datakache.api.metrics.receiver

/**
 * Metrics receiver for database operations, failures, and transaction events.
 */
interface ChangeStreamReceiver {
    // Change Stream Operations - Document Operations
    fun onChangeStreamInsert(cacheName: String, docKeyString: String)
    fun onChangeStreamReplace(cacheName: String, docKeyString: String)
    fun onChangeStreamUpdate(cacheName: String, docKeyString: String)
    fun onChangeStreamDelete(cacheName: String, docKeyString: String)

    // Change Stream Operations - Administrative Operations
    fun onChangeStreamDrop(cacheName: String)
    fun onChangeStreamRename(cacheName: String)
    fun onChangeStreamDropDatabase(cacheName: String)
    fun onChangeStreamInvalidate(cacheName: String)
    fun onChangeStreamUnknown(cacheName: String)
}
