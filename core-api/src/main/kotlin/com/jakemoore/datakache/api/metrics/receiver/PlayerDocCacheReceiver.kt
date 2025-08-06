package com.jakemoore.datakache.api.metrics.receiver

/**
 * Metrics receiver for PlayerDocCache operations and failures.
 *
 * These metrics only apply when using the `plugin-api` DataKache API.
 * If you are using the `core-api` DataKache API, these methods will not be invoked.
 */
interface PlayerDocCacheReceiver {
    // CRUD Operations
    fun onPlayerDocClear()
    fun onPlayerDocCreate()

    // Fail States
    fun onPlayerDocClearFail()
    fun onPlayerDocCreateFail()
}
