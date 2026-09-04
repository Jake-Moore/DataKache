package com.jakemoore.datakache.core.connections.queues

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.update.UpdateQueueShutdownException
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
internal class UpdateQueueManager(private val loggerService: LoggerService) : CoroutineScope {
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
     * Gets or creates the UpdateQueue for this document key and enqueues the update on it.
     *
     * @return The queue that took the request and the deferred that will resolve to the updated
     * document. The queue travels with it because a waiter must watch that instance rather than
     * whatever queue later answers to the same key; see [QueuedUpdate].
     */
    suspend fun <K : Any, D : Doc<K, D>> enqueueUpdate(
        docCache: DocCache<K, D>,
        doc: D,
        updateFunction: (D) -> D,
        updateExecutor: suspend (DocCache<K, D>, D, (D) -> D, Boolean) -> D,
        bypassValidation: Boolean,
    ): QueuedUpdate<K, D> {
        if (isShutdown.get()) {
            return refuse(docCache, doc.key)
        }

        val queueKey = QueueKey(docCache.cacheName, docCache.keyToString(doc.key))

        // Null once shutdown has begun. The check above is only a fast path; this one is the
        // authoritative answer, because it is made under the same lock shutdown snapshots with.
        val queue =
            getOrCreateQueue(queueKey, doc.key, docCache, updateExecutor)
                ?: return refuse(docCache, doc.key)

        // Enqueue the update
        return QueuedUpdate(queue, queue.enqueueUpdate(doc, updateFunction, bypassValidation))
    }

    /**
     * A request the manager would not accept, failed before it reached any queue.
     */
    private fun <K : Any, D : Doc<K, D>> refuse(docCache: DocCache<K, D>, docKey: K): QueuedUpdate<K, D> {
        val deferred = CompletableDeferred<D>()
        deferred.completeExceptionally(
            UpdateQueueShutdownException(docCache.getKeyNamespace(docKey)),
        )
        // getQueue, never getOrCreate. Creating one here would launch a processing coroutine on a
        // manager that is already shut down, parked forever on a channel nothing will send to,
        // once per document key touched during teardown.
        return QueuedUpdate(getQueue(docCache, docKey), deferred)
    }

    /**
     * A request and the queue that took it, or no queue when the manager was already shut down
     * and the request was failed without ever being enqueued.
     *
     * The queue travels with the request because a waiter has to watch **that** queue, not whatever
     * queue currently answers to the same document key. The idle sweep can retire a queue and a
     * later write create a fresh one under the same key, and a waiter comparing progress by key
     * would then be reading a different queue's counter: either one that has never run, so a
     * healthy request looks wedged, or a busy one, so a wedged request looks healthy.
     */
    internal class QueuedUpdate<K : Any, D : Doc<K, D>>(
        val queue: UpdateQueue<K, D>?,
        val deferred: CompletableDeferred<D>,
    )

    /**
     * Gets or creates an UpdateQueue for the specified document key.
     *
     * Null once [shutdown] has set its flag, so that no queue is created after the snapshot that
     * shutdown takes: such a queue is in no snapshot, is never shut down, and leaves a processing
     * coroutine running an executor against a service that has stopped.
     *
     * The lock-free read stays lock free. An entry found there was published before any snapshot,
     * so it is either in that snapshot or the map has since been cleared and the read misses.
     */
    @Suppress("UNCHECKED_CAST")
    private suspend fun <K : Any, D : Doc<K, D>> getOrCreateQueue(
        queueKey: QueueKey,
        documentKey: K,
        docCache: DocCache<K, D>,
        updateExecutor: suspend (DocCache<K, D>, D, (D) -> D, Boolean) -> D,
    ): UpdateQueue<K, D>? {
        // First check without lock (fast path)
        val existingQueue = queues[queueKey]
        if (existingQueue != null) {
            return existingQueue as UpdateQueue<K, D>
        }

        // Double-checked locking to prevent race conditions
        return queueMutex.withLock {
            // Shutdown sets its flag before taking this lock, so reading it here orders creation
            // against the snapshot: either this queue is in it, or this returns null.
            if (isShutdown.get()) {
                return@withLock null
            }

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
                    "(cache: ${docCache.cacheName})",
            )

            return@withLock newQueue
        }
    }

    /**
     * Gets the existing queue for the specified document key, or null if it doesn't exist.
     * This method does not create a new queue if one doesn't exist.
     */
    @Suppress("UNCHECKED_CAST")
    fun <K : Any, D : Doc<K, D>> getQueue(docCache: DocCache<K, D>, docKey: K): UpdateQueue<K, D>? {
        val queueKey = QueueKey(docCache.cacheName, docCache.keyToString(docKey))
        return queues[queueKey] as? UpdateQueue<K, D>
    }

    /**
     * Gets the queue size for the specified document key.
     * Returns 0 if no queue exists for the given key.
     */
    @Suppress("unused")
    fun <K : Any, D : Doc<K, D>> getQueueSize(docCache: DocCache<K, D>, docKey: K): Long {
        val queue = getQueue(docCache, docKey)
        return queue?.getQueueSize() ?: 0L
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
                                            "Error shutting down queue $queueKey during manager shutdown",
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
                            "Active queues: $activeQueues",
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

        // Under the lock, so it cannot interleave with a queue being created. Each queue is then
        // shut down outside it, since that waits on in-flight work and the idle sweep holds this
        // same lock.
        val activeQueues =
            queueMutex.withLock {
                val snapshot = queues.values.toList()
                queues.clear()
                snapshot
            }

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
    private data class QueueKey(val cacheName: String, val documentKeyString: String) {
        override fun toString(): String = "$cacheName::$documentKeyString"
    }
}
