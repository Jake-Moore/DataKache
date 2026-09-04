package com.jakemoore.datakache.api.metrics

/**
 * A snapshot of a cache's change stream buffer, for exporting as a gauge.
 *
 * Events are handed to a bounded buffer and applied from it in the database's commit order. A buffer
 * that fills does not drop or reorder anything, it pauses the stream, so depth approaching
 * [capacity] is the signal that the cache is falling behind the database.
 *
 * @param capacity How many events the buffer holds before the stream is paused.
 * @param depth How many events are waiting right now.
 * @param peakSinceLastRead The deepest the buffer has been since the previous snapshot was taken.
 * **Reading a snapshot resets this**, so consecutive reads describe consecutive intervals rather
 * than a running maximum. A gauge sampled every fifteen seconds misses the burst that fills a
 * thousand-event buffer in under one; this does not.
 */
data class ChangeStreamQueueStats(val capacity: Int, val depth: Int, val peakSinceLastRead: Int)
