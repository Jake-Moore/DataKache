package com.jakemoore.datakache.core.connections.queues

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
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

    // Channel for queuing update requests with fixed capacity
    private val updateChannel = Channel<UpdateRequest<K, D>>(capacity = MAX_QUEUED_UPDATES)

    // Metrics and state tracking
    private val isProcessing = AtomicBoolean(false)
    private val isShutdown = AtomicBoolean(false)

    // Queue size tracking - atomic counter for thread-safe access
    private val queueSize = AtomicLong(0)

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
     *
     * Implements backpressure handling when the queue is full:
     * - Attempts to send the update request to the channel
     * - If the channel is full, retries with short delays
     * - If all retries fail, rejects the update with an appropriate exception
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

        // Attempt to send to channel with backpressure handling
        val sendResult = updateChannel.trySend(request)

        when {
            sendResult.isSuccess -> {
                // Successfully queued - increment counter
                queueSize.incrementAndGet()
            }
            sendResult.isFailure -> {
                val exception = sendResult.exceptionOrNull()
                if (exception is kotlinx.coroutines.channels.ClosedSendChannelException) {
                    // Channel is closed (shutdown)
                    deferred.completeExceptionally(
                        IllegalStateException("UpdateQueue for key ${docCache.keyToString(key)} is shutdown")
                    )
                } else {
                    // Channel is full - implement backpressure strategy
                    handleBackpressure(request, deferred)
                }
            }
        }

        return deferred
    }

    /**
     * Handles backpressure when the update channel is full.
     * Attempts to retry sending the request with short delays.
     */
    @OptIn(DelicateCoroutinesApi::class)
    private fun handleBackpressure(
        request: UpdateRequest<K, D>,
        deferred: CompletableDeferred<D>
    ) {
        // Launch a coroutine to handle backpressure asynchronously
        scope.launch {
            try {
                var retryCount = 0

                while (retryCount < MAX_BACKPRESSURE_RETRIES && !updateChannel.isClosedForSend) {
                    delay(BACKPRESSURE_RETRY_DELAY_MS)

                    val retryResult = updateChannel.trySend(request)
                    if (retryResult.isSuccess) {
                        // Successfully queued after retry
                        queueSize.incrementAndGet()
                        return@launch
                    }

                    retryCount++
                }

                // All retries failed - reject the update
                val errorMessage = "UpdateQueue for key ${docCache.keyToString(key)} is full " +
                    "(capacity: $MAX_QUEUED_UPDATES). Update rejected after $MAX_BACKPRESSURE_RETRIES retry attempts."

                docCache.getLoggerInternal().warn(errorMessage)
                deferred.completeExceptionally(
                    IllegalStateException(errorMessage)
                )
            } catch (e: Exception) {
                // Handle any unexpected errors during backpressure handling
                docCache.getLoggerInternal().error(
                    e,
                    "Error during backpressure handling for key: ${docCache.keyToString(key)}"
                )
                deferred.completeExceptionally(e)
            }
        }
    }

    /**
     * Main processing loop that handles updates sequentially in FIFO order.
     */
    private suspend fun processUpdates() {
        docCache.getLoggerInternal().debug("Started UpdateQueue processing for key: ${docCache.keyToString(key)}")

        try {
            for (request in updateChannel) {
                // Decrement queued count as soon as the channel gives us a request
                queueSize.decrementAndGet()

                isProcessing.set(true)
                lastActivityTime.set(System.currentTimeMillis())

                try {
                    processUpdateRequest(request)
                } catch (e: CancellationException) {
                    request.deferred.completeExceptionally(
                        CancellationException(
                            "UpdateQueue processing cancelled for key: ${docCache.keyToString(key)}"
                        )
                    )
                    // rethrow for cooperative cancellation
                    throw e
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
        } catch (e: CancellationException) {
            // Don't log cancellation exceptions as errors during shutdown
            if (!isShutdown.get()) {
                docCache.getLoggerInternal().severe(
                    "UpdateQueue processing cancelled for key: ${docCache.keyToString(key)}"
                )
            }
            // rethrow for cooperative cancellation
            throw e
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

                // Drain remaining queued requests and complete them exceptionally
                while (true) {
                    val result = updateChannel.tryReceive()
                    if (result.isFailure) break
                    result.getOrNull()?.deferred?.completeExceptionally(
                        CancellationException(
                            "UpdateQueue shutdown forced for key: ${docCache.keyToString(key)}"
                        )
                    )
                }
            } finally {
                // Cancel the scope to clean up any other coroutines
                scope.cancel()

                // Wait for all coroutines in the scope to finish
                runCatching { scope.coroutineContext[Job]?.join() }

                docCache.getLoggerInternal().debug(
                    "UpdateQueue scope cancelled and cleaned up for key: ${docCache.keyToString(key)}"
                )
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

    /**
     * @return the current number of items waiting in the queue (not including the one being processed)
     */
    fun getQueueSize(): Long = queueSize.get()

    /**
     * @return the total number of items in the queue including the one being processed
     */
    fun getTotalQueueSize(): Long {
        val baseSize = queueSize.get()
        return if (isProcessing.get()) baseSize + 1 else baseSize
    }

    // TODO - Move to configuration system when available
    companion object {
        /**
         * Maximum number of update requests that can be queued per document.
         */
        const val MAX_QUEUED_UPDATES = 200

        /**
         * Maximum number of retry attempts when the queue is full.
         */
        private const val MAX_BACKPRESSURE_RETRIES = 3

        /**
         * Delay between retry attempts when the queue is full (milliseconds).
         */
        private const val BACKPRESSURE_RETRY_DELAY_MS = 50L
    }
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
