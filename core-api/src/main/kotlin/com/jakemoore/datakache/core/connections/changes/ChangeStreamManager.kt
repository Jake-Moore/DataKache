package com.jakemoore.datakache.core.connections.changes

import com.jakemoore.datakache.api.doc.Doc

/**
 * Manages change stream connections for database collections.
 * This interface abstracts the change stream functionality to allow for different implementations.
 */
interface ChangeStreamManager<K : Any, D : Doc<K, D>> {
    /**
     * Starts the change stream listener synchronously.
     * This method blocks until the change stream is fully initialized or fails.
     * @param startAtOperationTime Optional timestamp to start the stream from a specific point in time.
     * @throws Exception if startup fails
     */
    suspend fun start(startAtOperationTime: Any?)

    /**
     * Stops the change stream listener synchronously.
     * This method blocks until all resources are cleaned up.
     * @throws Exception if shutdown encounters critical errors
     */
    suspend fun stop()

    /**
     * Gets the current state of the change stream.
     */
    fun getCurrentState(): ChangeStreamState

    /**
     * Gets the last error that occurred, if any.
     */
    fun getLastError(): Throwable?

    /**
     * Gets the number of consecutive failures.
     */
    fun getConsecutiveFailures(): Int

    /**
     * Checks if the change stream job, and the event processor job are both active.
     */
    fun areJobsActive(): Boolean
}
