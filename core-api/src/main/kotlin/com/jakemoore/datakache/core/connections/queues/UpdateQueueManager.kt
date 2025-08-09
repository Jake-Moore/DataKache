package com.jakemoore.datakache.core.connections.queues

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.logging.LoggerService
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.cancellation.CancellationException

/**
 * Manages the lifecycle of [UpdateQueue]s for different document keys.
 * Provides automatic cleanup of idle queues and ensures proper resource management.
 *
 * This is a database-agnostic component that works with any
 * [com.jakemoore.datakache.core.connections.DatabaseService] implementation.
 */
internal class UpdateQueueManager(
    private val loggerService: LoggerService,
) : CoroutineScope {

    private val job = SupervisorJob()
    override val coroutineContext = Dispatchers.IO + job

    // Map of document keys to their respective queues
    // Using Any as key type since different caches can have different key types
    private val queues = ConcurrentHashMap<QueueKey, UpdateQueue<*, *>>()

    // Cleanup configuration
    private val idleTimeoutMs: Long = 30_000 // 30 seconds
    private val cleanupIntervalMs: Long = 60_000 // 1 minute

    // State tracking
    private val isShutdown = AtomicBoolean(false)

    // Mutex for thread-safe queue operations
    private val queueMutex = Mutex()

    init {
        // Start the cleanup task
        launch {
            cleanupIdleQueues()
        }
    }

    /**
     * Gets or creates an UpdateQueue for the specified document key.
     * Returns a CompletableDeferred that will resolve to the updated document.
     */
    suspend fun <K : Any, D : Doc<K, D>> enqueueUpdate(
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D,
        updateExecutor: suspend (DocCache<K, D>, D, (D) -> D, Boolean) -> D,
        bypassValidation: Boolean,
    ): CompletableDeferred<D> {
        if (isShutdown.get()) {
            val deferred = CompletableDeferred<D>()
            deferred.completeExceptionally(
                IllegalStateException("UpdateQueueManager is shutdown")
            )
            return deferred
        }

        val queueKey = QueueKey(docCache.cacheName, docCache.keyToString(doc.key))

        // Get or create the queue for this document key
        val queue = getOrCreateQueue(queueKey, doc.key, docCache, updateExecutor)

        // Enqueue the update
        return queue.enqueueUpdate(doc, updateFunction, bypassValidation)
    }

    /**
     * Gets an existing queue or creates a new one for the specified key.
     * Uses double-checked locking pattern to avoid race conditions.
     */
    @Suppress("UNCHECKED_CAST")
    private suspend fun <K : Any, D : Doc<K, D>> getOrCreateQueue(
        queueKey: QueueKey,
        documentKey: K,
        docCache: DocCache<K, D>,
        updateExecutor: suspend (DocCache<K, D>, D, (D) -> D, Boolean) -> D
    ): UpdateQueue<K, D> {
        // First check without lock (fast path)
        val existingQueue = queues[queueKey]
        if (existingQueue != null) {
            return existingQueue as UpdateQueue<K, D>
        }

        // Double-checked locking to prevent race conditions
        return queueMutex.withLock {
            // Check again inside the lock in case another thread created it
            val doubleCheckedQueue = queues[queueKey]
            if (doubleCheckedQueue != null) {
                return@withLock doubleCheckedQueue as UpdateQueue<K, D>
            }

            // Create new queue - we're the first thread to reach this point
            val newQueue = UpdateQueue(documentKey, docCache, updateExecutor)
            queues[queueKey] = newQueue

            docCache.getLoggerInternal().debug(
                "Created new UpdateQueue for key: ${docCache.keyToString(documentKey)} " +
                    "(cache: ${docCache.cacheName})"
            )

            return@withLock newQueue
        }
    }

    /**
     * Periodic cleanup task that removes idle queues to prevent memory leaks.
     */
    private suspend fun cleanupIdleQueues() {
        while (isActive && !isShutdown.get()) {
            try {
                delay(cleanupIntervalMs)

                if (isShutdown.get()) break

                val queuesToRemove = mutableListOf<QueueKey>()

                // Find idle queues
                queues.forEach { (queueKey, queue) ->
                    if (queue.isIdleForDuration(idleTimeoutMs)) {
                        queuesToRemove.add(queueKey)
                    }
                }

                // Remove idle queues
                if (queuesToRemove.isNotEmpty()) {
                    queueMutex.withLock {
                        queuesToRemove.forEach { queueKey ->
                            val queue = queues.remove(queueKey)
                            if (queue != null) {
                                // Only launch shutdown coroutine if we're still active
                                if (isActive) {
                                    launch {
                                        try {
                                            queue.shutdown()
                                        } catch (e: Exception) {
                                            // Log but don't fail the cleanup process
                                            loggerService.error(e, "Error shutting down queue $queueKey")
                                        }
                                    }
                                } else {
                                    // If we're shutting down, shutdown the queue directly
                                    try {
                                        queue.shutdown()
                                    } catch (e: Exception) {
                                        loggerService.error(
                                            e,
                                            "Error shutting down queue $queueKey during manager shutdown"
                                        )
                                    }
                                }
                            }
                        }
                    }

                    // Log cleanup results
                    val activeQueues = queues.size
                    loggerService.debug(
                        "UpdateQueueManager: Cleaned up ${queuesToRemove.size} idle queues. " +
                            "Active queues: $activeQueues"
                    )
                }
            } catch (_: CancellationException) {
                // Don't log cancellation exceptions as errors during shutdown
                loggerService.debug("UpdateQueueManager cleanup cancelled during shutdown")
            } catch (e: Exception) {
                loggerService.error(e, "Error during UpdateQueueManager cleanup")
                // Continue the cleanup loop even if there's an error
            }
        }
    }

    /**
     * Gracefully shuts down all queues and stops the manager.
     */
    suspend fun shutdown() {
        if (isShutdown.getAndSet(true)) {
            return // Already shutdown
        }

        loggerService.debug("UpdateQueueManager: Starting shutdown process...")

        // Cancel the cleanup job
        job.cancel()

        // Shutdown all existing queues
        val activeQueues = queues.values.toList()
        queues.clear()

        activeQueues.forEach { queue ->
            runCatching { queue.shutdown() }
                .onFailure { e ->
                    loggerService.error(e, "Error shutting down queue during manager shutdown")
                }
        }

        // Wait for the cleanup job to complete
        job.join()
    }

    /**
     * Returns the number of currently active queues for monitoring purposes.
     */
    fun getActiveQueuesCount(): Int = queues.size

    /**
     * Internal key class for identifying queues uniquely across different caches.
     */
    private data class QueueKey(
        val cacheName: String,
        val documentKeyString: String
    ) {
        override fun toString(): String = "$cacheName::$documentKeyString"
    }
}
