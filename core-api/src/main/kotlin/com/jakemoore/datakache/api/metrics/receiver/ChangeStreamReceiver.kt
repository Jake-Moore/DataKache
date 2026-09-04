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

    // Change Stream Operations - Buffer Pressure

    /**
     * The change stream's bounded buffer filled, so the stream is paused until it drains.
     *
     * Nothing is dropped or applied out of order when this happens, but the cache is behind the
     * database until it clears. For the depth itself rather than the alarm, poll
     * [com.jakemoore.datakache.api.cache.DocCache.getChangeStreamQueueStats].
     *
     * **Fires once per event that arrives while the buffer is full**, not once per episode, so a
     * sustained backlog calls this repeatedly. Count it rather than treating it as an edge.
     *
     * Has a default body, unlike the rest of this interface, so that adding it does not break
     * existing implementers.
     */
    fun onChangeStreamBackpressure(cacheName: String) {}
}
