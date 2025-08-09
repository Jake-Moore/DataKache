package com.jakemoore.datakache.core.connections.queues

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.cancellation.CancellationException

/**
 * A FIFO queue for processing document updates to ensure ordered execution and eliminate database-level conflicts.
 * Each document key gets its own queue to serialize updates while allowing concurrent updates to different documents.
 *
 * @param K The type of the document key
 * @param D The type of the document
 * @param docCache The document cache this queue is associated with
 * @param updateExecutor Function that performs the actual database update operation
 */
internal class UpdateQueue<K : Any, D : Doc<K, D>>(
    private val key: K,
    private val docCache: DocCache<K, D>,
    private val updateExecutor: suspend (DocCache<K, D>, D, (D) -> D, Boolean) -> D
) {

    // Separate job for the processing coroutine
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val processingJob: Job

    // Channel for queuing update requests
    // TODO need to set a limit (like Change Streams) and handle backpressure + rejection
    private val updateChannel = Channel<UpdateRequest<K, D>>(Channel.UNLIMITED)

    // Metrics and state tracking
    private val isProcessing = AtomicBoolean(false)
    private val isShutdown = AtomicBoolean(false)

    // Cleanup tracking
    private val lastActivityTime = AtomicLong(System.currentTimeMillis())

    // Mutex for coordinating shutdown
    private val shutdownMutex = Mutex()

    init {
        // Start the processing coroutine with its own job
        processingJob = scope.launch {
            processUpdates()
        }
    }

    /**
     * Queues an update operation for this document key.
     * Returns a [CompletableDeferred] that will complete with the updated document or exception.
     */
    fun enqueueUpdate(
        doc: D,
        updateFunction: (D) -> D,
        bypassValidation: Boolean,
    ): CompletableDeferred<D> {
        require(doc.key == key) {
            "Enqueued doc key does not match UpdateQueue key"
        }

        val deferred = CompletableDeferred<D>()
        val request = UpdateRequest(doc, updateFunction, deferred, bypassValidation)

        if (isShutdown.get()) {
            deferred.completeExceptionally(
                IllegalStateException("UpdateQueue for key ${docCache.keyToString(key)} is shutdown")
            )
            return deferred
        }

        // Update activity time
        lastActivityTime.set(System.currentTimeMillis())

        // Add to queue - this will suspend if the queue is full (shouldn't happen with unlimited channel)
        val sent = updateChannel.trySend(request)
        if (!sent.isSuccess) {
            val exception = sent.exceptionOrNull() ?: IllegalStateException(
                "Failed to enqueue update request for key ${docCache.keyToString(key)}"
            )
            deferred.completeExceptionally(exception)
        }

        return deferred
    }

    /**
     * Main processing loop that handles updates sequentially in FIFO order.
     */
    private suspend fun processUpdates() {
        docCache.getLoggerInternal().debug("Started UpdateQueue processing for key: ${docCache.keyToString(key)}")

        try {
            for (request in updateChannel) {
                isProcessing.set(true)
                lastActivityTime.set(System.currentTimeMillis())

                try {
                    processUpdateRequest(request)
                } catch (e: Exception) {
                    docCache.getLoggerInternal().error(
                        e,
                        "Unexpected error processing update request for key: ${docCache.keyToString(key)}"
                    )
                    request.deferred.completeExceptionally(e)
                } finally {
                    isProcessing.set(false)
                }
            }
        } catch (_: CancellationException) {
            // Don't log cancellation exceptions as errors during shutdown
            if (!isShutdown.get()) {
                docCache.getLoggerInternal().severe(
                    "UpdateQueue processing cancelled for key: ${docCache.keyToString(key)}"
                )
            }
        } catch (e: Exception) {
            docCache.getLoggerInternal().error(
                e,
                "UpdateQueue processing loop terminated with error for key: ${docCache.keyToString(key)}"
            )
        } finally {
            docCache.getLoggerInternal().debug("UpdateQueue processing ended for key: ${docCache.keyToString(key)}")
        }
    }

    /**
     * Processes a single update request, handling all exceptions appropriately.
     */
    private suspend fun processUpdateRequest(request: UpdateRequest<K, D>) {
        try {
            // Execute the actual database update
            val updatedDoc = updateExecutor(docCache, request.doc, request.updateFunction, request.bypassValidation)

            // Complete the deferred with success
            request.deferred.complete(updatedDoc)
        } catch (e: Exception) {
            // Promote exception to the deferred result
            request.deferred.completeExceptionally(e)
        }
    }

    /**
     * Gracefully shuts down this queue, completing any remaining updates.
     * This method ensures all pending updates are processed before shutdown.
     *
     * @param timeoutMs Maximum time to wait for pending updates to complete (default: 30 seconds)
     */
    suspend fun shutdown(timeoutMs: Long = 30_000) {
        shutdownMutex.withLock {
            if (isShutdown.getAndSet(true)) {
                return // Already shutdown
            }

            docCache.getLoggerInternal().debug(
                "Shutting down UpdateQueue for key: ${docCache.keyToString(key)} - " +
                    "will complete all pending updates (timeout: ${timeoutMs}ms)"
            )

            // Step 1: Close the channel to prevent new updates
            updateChannel.close()

            // Step 2: Wait for the processing job to naturally complete all pending updates
            // with a timeout for emergency situations
            try {
                withTimeout(timeoutMs) {
                    // Give some time for processing to finish
                    processingJob.join()
                }
                docCache.getLoggerInternal().debug(
                    "UpdateQueue graceful shutdown complete for key: ${docCache.keyToString(key)}"
                )
            } catch (_: TimeoutCancellationException) {
                docCache.getLoggerInternal().error(
                    "UpdateQueue shutdown timed out after ${timeoutMs}ms for key: ${docCache.keyToString(key)} - " +
                        "forcing cancellation"
                )
                // Force cancellation if timeout exceeded
                processingJob.cancel()
                runCatching { processingJob.join() }
            }
        }
    }

    /**
     * @return true if this queue has been idle for the specified duration
     */
    fun isIdleForDuration(durationMs: Long): Boolean {
        return !isProcessing.get() &&
            (System.currentTimeMillis() - lastActivityTime.get()) > durationMs
    }

    /**
     * @return true if this queue is currently processing an update
     */
    @Suppress("unused")
    fun isCurrentlyProcessing(): Boolean = isProcessing.get()
}

/**
 * Internal data class representing an update request in the queue.
 */
private data class UpdateRequest<K : Any, D : Doc<K, D>>(
    val doc: D,
    val updateFunction: (D) -> D,
    val deferred: CompletableDeferred<D>,
    val bypassValidation: Boolean,
)
