package com.jakemoore.datakache.core.connections.mongo

import com.jakemoore.datakache.api.coroutines.DataKacheScope
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.core.connections.changes.ChangeEventHandler
import com.jakemoore.datakache.core.connections.changes.ChangeOperationType
import com.jakemoore.datakache.core.connections.changes.ChangeStreamConfig
import com.jakemoore.datakache.core.connections.changes.ChangeStreamManager
import com.jakemoore.datakache.core.connections.changes.ChangeStreamState
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.client.model.changestream.OperationType
import com.mongodb.kotlin.client.coroutine.MongoCollection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import org.bson.BsonDocument
import org.bson.BsonTimestamp
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.min
import kotlin.math.pow

class MongoChangeStreamManager<K : Any, D : Doc<K, D>>(
    private val collection: MongoCollection<D>,
    private val eventHandler: ChangeEventHandler<K, D>,
    private val config: ChangeStreamConfig = ChangeStreamConfig.forMongoDB(),
    private val logger: LoggerService
) : ChangeStreamManager<K, D>, DataKacheScope {

    private val state = AtomicReference(ChangeStreamState.DISCONNECTED)
    private val stateMutex = Mutex() // Thread-safe state management
    private var changeStreamJob: Job? = null
    private var eventProcessorJob: Job? = null

    // Resume token management with proper fallback chain
    private var resumeToken: BsonDocument? = null
    private var lastResumeToken: BsonDocument? = null
    private var effectiveStartTime: BsonTimestamp? = null

    // Event processing with backpressure - recreated in start()
    private var eventChannel: Channel<ChangeStreamDocument<D>>? = null

    // For monitoring and debugging
    private var consecutiveFailures = 0
    private var lastError: Throwable? = null
    private var totalEventsProcessed = 0L
    private var lastTokenCleanupTime = System.currentTimeMillis()

    /**
     * Starts the change stream listener. If a startAtOperationTime is provided,
     * the stream will start from that point to avoid missing changes.
     * This method is thread-safe and completes all setup before returning.
     */
    override suspend fun start(startAtOperationTime: Any?) {
        startInternal(startAtOperationTime)
    }

    /**
     * Internal suspend implementation of start that completes setup work.
     */
    private suspend fun startInternal(startAtOperationTime: Any?) {
        stateMutex.withLock {
            val currentState = state.get()
            if (currentState == ChangeStreamState.CONNECTED ||
                currentState == ChangeStreamState.CONNECTING ||
                currentState == ChangeStreamState.RECONNECTING
            ) {
                logger.warn(
                    "Cannot start change stream for ${collection.namespace.collectionName} " +
                        "- already in state: $currentState"
                )
                return@withLock
            }

            // Simple state transition
            val currentStateSnapshot = state.get()
            val canStart = when (currentStateSnapshot) {
                ChangeStreamState.DISCONNECTED, ChangeStreamState.FAILED, ChangeStreamState.SHUTDOWN -> true
                else -> false
            }

            if (!canStart) {
                logger.warn(
                    "Cannot start change stream for ${collection.namespace.collectionName} " +
                        "- already in state: $currentStateSnapshot"
                )
                return@withLock
            }

            if (!state.compareAndSet(currentStateSnapshot, ChangeStreamState.CONNECTING)) {
                logger.error(
                    "Failed to transition to CONNECTING state for ${collection.namespace.collectionName} " +
                        "- concurrent state change detected"
                )
                return@withLock
            }

            try {
                // Set the effective start time from the provided parameter (critical timing gap fix)
                this@MongoChangeStreamManager.effectiveStartTime = startAtOperationTime as? BsonTimestamp
                logger.info(
                    "Starting change stream for ${collection.namespace.collectionName} " +
                        "with operation time: $effectiveStartTime"
                )

                // CRITICAL FIX: Recreate the event channel since closed channels cannot be reused
                createNewEventChannel()

                // Reset counters and state for restart
                resetCountersForRestart()

                // Start the event processor first
                startEventProcessor()

                // Then start the change stream watcher
                changeStreamJob = this@MongoChangeStreamManager.launch {
                    try {
                        startChangeStreamWithRetry()
                    } catch (e: Exception) {
                        logger.error(e, "Fatal error in change stream for ${collection.namespace.collectionName}")
                        state.set(ChangeStreamState.FAILED)
                        clearResourcesUnsafe()
                        throw e
                    }
                }

                logger.info("Change stream jobs started for ${collection.namespace.collectionName}")
            } catch (e: Exception) {
                logger.error(e, "Failed to start change stream for ${collection.namespace.collectionName}")
                state.set(ChangeStreamState.FAILED)
                clearResourcesUnsafe()
                throw e
            }
        }
    }

    /**
     * Creates a new event channel, replacing any existing one.
     * Critical for restart scenarios since closed channels cannot be reused.
     */
    private fun createNewEventChannel() {
        eventChannel?.close() // Close existing channel if any
        eventChannel = Channel(capacity = config.maxBufferedEvents)
        logger.debug("Created new event channel with capacity ${config.maxBufferedEvents}")
    }

    /**
     * Resets counters and state for restart scenarios.
     */
    private fun resetCountersForRestart() {
        // Reset events counter to prevent overflow and provide fresh start
        totalEventsProcessed = 0L
        lastTokenCleanupTime = System.currentTimeMillis()
        logger.debug("Reset counters for restart")
    }

    /**
     * Stops the change stream listener.
     * This method is thread-safe and completes all cleanup before returning.
     */
    override suspend fun stop() {
        stopInternal()
    }

    /**
     * Internal suspend implementation of stop that completes cleanup work.
     */
    private suspend fun stopInternal() {
        stateMutex.withLock {
            val currentState = state.get()
            if (currentState == ChangeStreamState.SHUTDOWN) {
                logger.debug("Change stream already shutdown for ${collection.namespace.collectionName}")
                return@withLock // Already shutdown
            }

            // Transition to shut down state
            if (!state.compareAndSet(currentState, ChangeStreamState.SHUTDOWN)) {
                logger.warn(
                    "Failed to transition to SHUTDOWN state for ${collection.namespace.collectionName} " +
                        "- attempted from: $currentState, current state: ${state.get()}"
                )
                return@withLock
            }

            logger.info("Shutting down change stream for ${collection.namespace.collectionName}")

            // CRITICAL FIX: Proper resource cleanup with job completion waiting
            cleanupResourcesWithJobCompletion()

            logger.info("Change stream shutdown completed for ${collection.namespace.collectionName}")
        }
    }

    /**
     * Starts the event processor that handles events from the channel with timeout and backpressure.
     */
    @OptIn(ExperimentalCoroutinesApi::class, DelicateCoroutinesApi::class)
    private fun startEventProcessor() {
        eventProcessorJob = this.launch {
            logger.debug("Event processor started for ${collection.namespace.collectionName}")

            try {
                while (state.get() != ChangeStreamState.SHUTDOWN) {
                    try {
                        // CRITICAL FIX: Prevent excessive CPU usage with very small timeouts
                        val baseInterval = config.eventProcessingTimeout.inWholeMilliseconds / 10
                        val checkInterval = minOf(maxOf(baseInterval, 100), 5000) // Min 100ms, max 5s

                        // CRITICAL FIX: Use local variable for null safety
                        val currentChannel = eventChannel
                        val event = select {
                            if (currentChannel != null && !currentChannel.isClosedForReceive) {
                                currentChannel.onReceive { it }
                            }
                            onTimeout(checkInterval) { null }
                        }

                        if (event != null) {
                            withTimeout(config.eventProcessingTimeout) {
                                processChangeEventSafely(event)

                                // CRITICAL FIX: Better overflow protection using >= instead of ==
                                totalEventsProcessed = if (totalEventsProcessed >= Long.MAX_VALUE - 1) {
                                    logger.debug("Events counter approaching max value, resetting to 0")
                                    0L
                                } else {
                                    totalEventsProcessed + 1
                                }

                                // Update resume tokens after successful processing
                                updateResumeTokens(event.resumeToken)

                                // Periodic cleanup
                                performPeriodicMaintenance()
                            }
                        }
                    } catch (_: TimeoutCancellationException) {
                        logger.debug("Event processing timeout for ${collection.namespace.collectionName}")
                    } catch (_: CancellationException) {
                        logger.debug("Event processor cancelled for ${collection.namespace.collectionName}")
                        break
                    } catch (e: Exception) {
                        // Classify exceptions to determine if processor should continue
                        if (shouldEventProcessorStop(e)) {
                            logger.error(
                                e,
                                "Fatal error in event processor, " +
                                    "stopping: ${collection.namespace.collectionName}"
                            )
                            break
                        } else {
                            logger.error(
                                e,
                                "Recoverable error in event processor " +
                                    "for ${collection.namespace.collectionName}"
                            )
                        }
                    }
                }
            } finally {
                logger.debug("Event processor stopped for ${collection.namespace.collectionName}")
            }
        }
    }

    /**
     * Determines if the event processor should stop based on the exception type.
     */
    private fun shouldEventProcessorStop(e: Exception): Boolean {
        val errorMessage = e.message?.lowercase() ?: ""

        return when {
            // Serialization/deserialization errors suggest data corruption
            errorMessage.contains("serialization") ||
                errorMessage.contains("deserialization") ||
                errorMessage.contains("class cast") -> true

            // Channel closed - normal shutdown
            errorMessage.contains("channel was closed") ||
                errorMessage.contains("channel is closed") -> true

            // Other errors are generally recoverable
            else -> false
        }
    }

    /**
     * Proper resource cleanup that waits for jobs to complete before cleaning up.
     * This method should only be called while holding the stateMutex.
     */
    private suspend fun cleanupResourcesWithJobCompletion() {
        logger.debug("Starting resource cleanup for ${collection.namespace.collectionName}")

        val criticalErrors = mutableListOf<Exception>()

        // CRITICAL FIX: Enhanced job cleanup with local variables for null safety
        val streamJob = changeStreamJob
        if (streamJob != null) {
            try {
                streamJob.cancel()
                streamJob.join() // Wait for job to complete
                logger.debug("Change stream job completed")
            } catch (e: Exception) {
                if (isCriticalCleanupError(e)) {
                    criticalErrors.add(e)
                    logger.error(e, "Critical error during change stream job cleanup")
                } else {
                    logger.warn("Non-critical error waiting for change stream job completion: ${e.message}")
                }
            }
        }

        val processorJob = eventProcessorJob
        if (processorJob != null) {
            try {
                processorJob.cancel()
                processorJob.join() // Wait for job to complete
                logger.debug("Event processor job completed")
            } catch (e: Exception) {
                if (isCriticalCleanupError(e)) {
                    criticalErrors.add(e)
                    logger.error(e, "Critical error during event processor job cleanup")
                } else {
                    logger.warn("Non-critical error waiting for event processor job completion: ${e.message}")
                }
            }
        }

        // Now safely close the channel
        val channel = eventChannel
        if (channel != null) {
            try {
                channel.close()
                logger.debug("Event channel closed")
            } catch (e: Exception) {
                logger.warn("Error closing event channel: ${e.message}")
            }
        }

        // Clear state (but keep tokens for potential restart)
        changeStreamJob = null
        eventProcessorJob = null
        eventChannel = null
        consecutiveFailures = 0
        lastError = null

        logger.debug("Resource cleanup completed for ${collection.namespace.collectionName}")

        // CRITICAL FIX: Propagate critical cleanup errors
        if (criticalErrors.isNotEmpty()) {
            val primaryError = criticalErrors.first()
            logger.error("Propagating critical cleanup error: ${primaryError.message}")
            throw primaryError
        }
    }

    /**
     * Determines if a cleanup error is critical and should be propagated.
     */
    private fun isCriticalCleanupError(e: Exception): Boolean {
        val errorMessage = e.message?.lowercase() ?: ""

        return when {
            // Resource leaks or corruption
            errorMessage.contains("resource leak") ||
                errorMessage.contains("memory") ||
                errorMessage.contains("corruption") -> true

            // Deadlocks or threading issues
            errorMessage.contains("deadlock") ||
                errorMessage.contains("interrupted") && !errorMessage.contains("cancellation") -> true

            // Normal cancellation or timeout is not critical
            errorMessage.contains("cancellation") ||
                errorMessage.contains("timeout") -> false

            // Unknown errors are not critical for cleanup
            else -> false
        }
    }

    /**
     * Handles event loss scenarios with fallback strategies.
     */
    private suspend fun handleEventLossScenario(change: ChangeStreamDocument<D>) {
        logger.error(
            "Event lost due to backpressure for ${collection.namespace.collectionName}, " +
                "operation: ${change.operationType}, implementing fallback strategy"
        )

        // CRITICAL FIX: Implement fallback strategies instead of just logging
        try {
            when (change.operationType) {
                OperationType.INSERT, OperationType.UPDATE, OperationType.REPLACE -> {
                    // For data changes, we could trigger a selective cache refresh
                    val doc = change.fullDocument
                    if (doc != null) {
                        logger.warn("Triggering direct cache update for lost event: ${doc.key}")
                        // Process the event directly to maintain consistency
                        processChangeEventSafely(change)
                    } else {
                        logger.error("Cannot recover from lost event - no fullDocument available")
                    }
                }
                OperationType.DELETE -> {
                    // For deletions, we can extract the key and handle directly
                    val documentKey = change.documentKey
                    if (documentKey != null) {
                        val id = extractIdFromDocumentKey(documentKey)
                        if (id != null) {
                            try {
                                @Suppress("UNCHECKED_CAST")
                                eventHandler.onDocumentDeleted(id as K)
                                logger.warn("Recovered from lost DELETE event for document: $id")
                            } catch (_: ClassCastException) {
                                logger.error(
                                    "Type mismatch in document ID during DELETE recovery: " +
                                        "expected type K, got ${id::class.simpleName}"
                                )
                            }
                        }
                    }
                }
                else -> {
                    logger.error("Cannot recover from lost event of type: ${change.operationType}")
                }
            }
        } catch (e: Exception) {
            logger.error(e, "Failed to recover from lost event - cache consistency may be compromised")
        }
    }

    /**
     * Legacy unsafe cleanup method for error scenarios.
     * This method should only be called while holding the stateMutex.
     */
    private fun clearResourcesUnsafe() {
        logger.debug("Starting unsafe resource cleanup for ${collection.namespace.collectionName}")

        // Cancel jobs without waiting
        changeStreamJob?.cancel()
        eventProcessorJob?.cancel()

        // Close channel
        eventChannel?.close()

        // Clear state (but keep tokens for potential restart)
        changeStreamJob = null
        eventProcessorJob = null
        eventChannel = null
        consecutiveFailures = 0
        lastError = null

        logger.debug("Unsafe resource cleanup completed for ${collection.namespace.collectionName}")
    }

    /**
     * Performs periodic maintenance like token cleanup.
     */
    private fun performPeriodicMaintenance() {
        val now = System.currentTimeMillis()
        if (now - lastTokenCleanupTime > 300_000) { // Every 5 minutes
            // Clean up old tokens if we have successful processing
            if (totalEventsProcessed % 1000 == 0L && resumeToken != null) {
                lastResumeToken = null // Clear very old token
                lastTokenCleanupTime = now
                logger.debug("Performed token cleanup for ${collection.namespace.collectionName}")
            }
        }
    }

    /**
     * Gets the current state of the change stream.
     */
    override fun getCurrentState(): ChangeStreamState = state.get()

    /**
     * Gets the last error that occurred, if any.
     */
    override fun getLastError(): Throwable? = lastError

    /**
     * Gets the number of consecutive failures.
     */
    override fun getConsecutiveFailures(): Int = consecutiveFailures

    /**
     * Configures the MongoDB change stream with proper settings and enhanced fallback chain.
     * Implements: resumeToken → lastResumeToken → effectiveStartTime → current time
     */
    @Suppress("KotlinConstantConditions")
    private fun configureChangeStream(): kotlinx.coroutines.flow.Flow<ChangeStreamDocument<D>> {
        val watchBuilder = collection.watch(pipeline = emptyList<BsonDocument>()).apply {
            try {
                // Always enable full document retrieval for UPDATE operations (improves cache accuracy)
                fullDocument(FullDocument.UPDATE_LOOKUP)
            } catch (e: Exception) {
                logger.warn("Could not set fullDocument mode, proceeding without it: ${e.message}")
            }

            // Enhanced fallback chain for stream positioning
            var configured = false

            // First try: Current resume token
            val resumeToken = resumeToken
            if (!configured && resumeToken != null) {
                try {
                    resumeAfter(resumeToken)
                    logger.info(
                        "Resuming change stream from current resume token for ${collection.namespace.collectionName}"
                    )
                    configured = true
                } catch (e: Exception) {
                    logger.warn("Current resume token failed: ${e.message}")
                    handleSpecificResumeTokenError(e)
                }
            }

            // Second try: Last resume token fallback
            val lastResumeToken = lastResumeToken
            if (!configured && lastResumeToken != null) {
                try {
                    resumeAfter(lastResumeToken)
                    logger.info(
                        "Resuming change stream from last resume token for ${collection.namespace.collectionName}"
                    )
                    configured = true
                } catch (e: Exception) {
                    logger.warn("Last resume token failed: ${e.message}")
                    handleSpecificResumeTokenError(e)
                }
            }

            // Third try: Operation time fallback
            val effectiveStartTime = effectiveStartTime
            if (!configured && effectiveStartTime != null) {
                try {
                    startAtOperationTime(effectiveStartTime)
                    logger.info(
                        "Starting change stream from operation time $effectiveStartTime " +
                            "for ${collection.namespace.collectionName}"
                    )
                    configured = true
                } catch (e: Exception) {
                    logger.warn("Operation time failed: ${e.message}")
                }
            }

            // Last resort: Current time
            if (!configured) {
                logger.warn(
                    "All fallback options failed, starting change stream from current time " +
                        "for ${collection.namespace.collectionName}"
                )
            }
        }

        return watchBuilder
    }

    /**
     * Handles specific resume token errors with granular error detection.
     */
    private fun handleSpecificResumeTokenError(e: Exception) {
        val errorMessage = e.message?.lowercase() ?: ""

        when {
            // Specific resume token errors that require token clearing
            errorMessage.contains("resume point may no longer be in the oplog") ||
                errorMessage.contains("invalid resume point") ||
                errorMessage.contains("resume token") && errorMessage.contains("invalid") -> {
                logger.warn("Resume token invalidated due to specific error, clearing tokens: ${e.message}")
                clearResumeTokensOnly()
            }

            // Network or temporary errors - keep tokens for retry
            errorMessage.contains("connection") ||
                errorMessage.contains("timeout") ||
                errorMessage.contains("network") -> {
                logger.warn("Network error with resume token, keeping tokens for retry: ${e.message}")
            }

            // Unknown resume token error - be conservative and clear
            errorMessage.contains("resume") -> {
                logger.warn("Unknown resume token error, clearing tokens: ${e.message}")
                clearResumeTokensOnly()
            }

            else -> {
                logger.warn("General error with change stream configuration: ${e.message}")
            }
        }
    }

    /**
     * Clears only resume tokens, preserving other state.
     */
    private fun clearResumeTokensOnly() {
        resumeToken = null
        lastResumeToken = null
    }

    /**
     * Main change stream loop with retry logic.
     */
    private suspend fun startChangeStreamWithRetry() {
        var retryCount = 0

        while (state.get() != ChangeStreamState.SHUTDOWN && retryCount < config.maxRetries) {
            try {
                state.set(ChangeStreamState.CONNECTING)
                logger.info(
                    "Starting change stream for ${collection.namespace.collectionName} (attempt ${retryCount + 1})"
                )

                val watchFlow = configureChangeStream()

                processChangeStreamEvents(watchFlow)

                // If we reach here, the stream ended normally (which shouldn't happen)
                logger.warn("Change stream ended normally for ${collection.namespace.collectionName}")
                break
            } catch (_: CancellationException) {
                logger.info("Change stream cancelled for ${collection.namespace.collectionName}")
                break
            } catch (e: Exception) {
                if (handleStreamError(e, retryCount)) {
                    break
                }
                retryCount++
            }
        }

        if (retryCount >= config.maxRetries) {
            state.set(ChangeStreamState.FAILED)
            logger.error(
                "Change stream failed permanently for ${collection.namespace.collectionName} " +
                    "after $retryCount attempts"
            )
        } else if (state.get() != ChangeStreamState.SHUTDOWN) {
            state.set(ChangeStreamState.DISCONNECTED)
        }
    }

    /**
     * Processes change stream events from the MongoDB watch flow with improved backpressure handling.
     */
    @OptIn(DelicateCoroutinesApi::class)
    private suspend fun processChangeStreamEvents(watchFlow: kotlinx.coroutines.flow.Flow<ChangeStreamDocument<D>>) {
        watchFlow.collect { change: ChangeStreamDocument<D> ->
            if (state.get() == ChangeStreamState.SHUTDOWN) {
                return@collect // Exit if shutdown was requested
            }

            // Update state to connected on first successful event
            if (state.compareAndSet(ChangeStreamState.CONNECTING, ChangeStreamState.CONNECTED) ||
                state.compareAndSet(ChangeStreamState.RECONNECTING, ChangeStreamState.CONNECTED)
            ) {
                onSuccessfulConnection()
            }

            // CRITICAL FIX: Enhanced backpressure strategy with event loss prevention
            val channel = eventChannel
            if (channel != null && !channel.isClosedForSend) {
                val sendResult = channel.trySend(change)

                when {
                    sendResult.isSuccess -> {
                        // Event sent successfully, continue
                    }
                    sendResult.isFailure -> {
                        val exception = sendResult.exceptionOrNull()
                        if (exception is kotlinx.coroutines.channels.ClosedSendChannelException) {
                            logger.debug("Channel closed, stopping event processing")
                            return@collect
                        }

                        // Channel is full - implement enhanced backpressure with fallback
                        logger.warn(
                            "Event channel full (${config.maxBufferedEvents} events), " +
                                "implementing backpressure strategy for ${collection.namespace.collectionName}"
                        )

                        // Strategy: Try a few times with short delays, then implement fallback
                        var retryCount = 0
                        val maxRetries = 3
                        var eventHandled = false

                        while (retryCount < maxRetries && !channel.isClosedForSend) {
                            delay(50) // Short delay to allow processing
                            val retryResult = channel.trySend(change)
                            if (retryResult.isSuccess) {
                                eventHandled = true
                                break
                            }
                            retryCount++
                        }

                        if (!eventHandled) {
                            // CRITICAL FIX: Implement fallback strategy instead of losing events
                            handleEventLossScenario(change)
                        }
                    }
                }
            } else {
                logger.debug("Event channel unavailable, skipping event")
                // Also handle this as a potential event loss scenario
                handleEventLossScenario(change)
            }
        }
    }

    /**
     * Handles successful connection to the change stream.
     */
    private suspend fun onSuccessfulConnection() {
        if (consecutiveFailures > 0) {
            logger.info(
                "Change stream reconnected after $consecutiveFailures failures " +
                    "for ${collection.namespace.collectionName}"
            )
        }

        consecutiveFailures = 0
        lastError = null

        eventHandler.onConnected()
        logger.info("Change stream connected for ${collection.namespace.collectionName}")
    }

    /**
     * Updates resume tokens for reconnection purposes.
     */
    private fun updateResumeTokens(newResumeToken: BsonDocument?) {
        lastResumeToken = resumeToken
        resumeToken = newResumeToken
    }

    /**
     * Handles errors during change stream processing.
     * @param e The exception that occurred
     * @param retryCount Current retry attempt number
     * @return true if the retry loop should break, false to continue retrying
     */
    private suspend fun handleStreamError(e: Exception, retryCount: Int): Boolean {
        consecutiveFailures++
        lastError = e

        logger.warn(
            "Change stream error for ${collection.namespace.collectionName} " +
                "(failure #$consecutiveFailures): ${e.message}"
        )

        if (state.get() == ChangeStreamState.SHUTDOWN) {
            return true
        }

        // Handle specific error types
        if (isFatalError(e)) {
            logger.error("Fatal error detected, stopping change stream for ${collection.namespace.collectionName}")
            state.set(ChangeStreamState.FAILED)
            return true
        }

        // Handle resume token invalidation errors
        handleResumeTokenErrors(e)

        // Notify handler of disconnection
        if (state.get() == ChangeStreamState.CONNECTED) {
            eventHandler.onDisconnected()
        }

        state.set(ChangeStreamState.RECONNECTING)

        // Calculate and apply retry delay
        return applyRetryDelay(retryCount)
    }

    /**
     * Determines if an error is fatal and should stop all retry attempts.
     * Enhanced with more nuanced error classification.
     */
    private fun isFatalError(e: Exception): Boolean {
        val errorMessage = e.message?.lowercase() ?: ""

        return when {
            // Definite fatal errors - authentication/authorization
            errorMessage.contains("authentication failed") ||
                errorMessage.contains("authorization failed") ||
                errorMessage.contains("access denied") ||
                errorMessage.contains("unauthorized") -> {
                logger.error("Fatal authentication/authorization error: ${e.message}")
                true
            }

            // MongoDB version/feature incompatibility
            errorMessage.contains("change streams are only supported") ||
                errorMessage.contains("feature is not supported") -> {
                logger.error("Fatal feature incompatibility error: ${e.message}")
                true
            }

            // Database/collection doesn't exist
            errorMessage.contains("database does not exist") ||
                errorMessage.contains("collection does not exist") -> {
                logger.error("Fatal resource not found error: ${e.message}")
                true
            }

            // Potentially recoverable errors that we used to consider fatal
            errorMessage.contains("connection") ||
                errorMessage.contains("timeout") ||
                errorMessage.contains("network") ||
                errorMessage.contains("host unreachable") -> {
                logger.warn("Network error, will retry: ${e.message}")
                false
            }

            else -> {
                // Unknown errors - be conservative but not fatal initially
                logger.warn("Unknown error, will retry with caution: ${e.message}")
                false
            }
        }
    }

    /**
     * Handles resume token related errors with specific error matching.
     */
    private fun handleResumeTokenErrors(e: Exception) {
        val errorMessage = e.message?.lowercase() ?: ""

        // Only clear tokens for specific resume token errors
        if (errorMessage.contains("resume point may no longer be in the oplog") ||
            errorMessage.contains("invalid resume point") ||
            (errorMessage.contains("resume token") && errorMessage.contains("invalid"))
        ) {
            logger.warn("Resume token invalidated due to specific error, clearing tokens: ${e.message}")
            clearResumeTokensOnly()
        } else if (errorMessage.contains("resume")) {
            logger.warn("General resume error, clearing tokens as precaution: ${e.message}")
            clearResumeTokensOnly()
        }
        // For non-resume errors, keep tokens for potential recovery
    }

    /**
     * Applies retry delay with exponential backoff and jitter.
     * Enhanced with overflow protection and better logging.
     * @param retryCount Current retry attempt number
     * @return true if the retry loop should break (due to cancellation), false to continue
     */
    private suspend fun applyRetryDelay(retryCount: Int): Boolean {
        // Prevent overflow in exponentiation by capping the exponent
        val safeRetryCount = min(retryCount, ChangeStreamConfig.MAX_BACKOFF_EXPONENT)

        // compute backoff multiplier as Double to avoid Long×Long overflow
        val multiplier = ChangeStreamConfig.BACKOFF_MULTIPLIER.pow(safeRetryCount)
        val baseDelay = if (config.initialRetryDelay.inWholeMilliseconds <= 0L ||
            multiplier > (Long.MAX_VALUE.toDouble() / config.initialRetryDelay.inWholeMilliseconds)
        ) {
            config.maxRetryDelay.inWholeMilliseconds
        } else {
            min(
                (config.initialRetryDelay.inWholeMilliseconds * multiplier).toLong(),
                config.maxRetryDelay.inWholeMilliseconds
            )
        }

        // Add jitter to prevent thundering herd
        val jitter = (baseDelay * ChangeStreamConfig.JITTER_FACTOR * kotlin.random.Random.nextDouble()).toLong()
        val delayMs = baseDelay + jitter

        logger.info(
            "Retrying change stream for ${collection.namespace.collectionName} " +
                "in ${delayMs}ms (attempt ${retryCount + 1}/${config.maxRetries}, " +
                "consecutive failures: $consecutiveFailures)"
        )

        return try {
            delay(delayMs)
            false // Continue retrying
        } catch (_: CancellationException) {
            logger.info("Retry cancelled for ${collection.namespace.collectionName}")
            true // Break the retry loop
        }
    }

    /**
     * Converts MongoDB's OperationType to our database-agnostic ChangeOperationType.
     */
    private fun mapOperationType(mongoOperationType: OperationType): ChangeOperationType {
        return when (mongoOperationType) {
            OperationType.INSERT -> ChangeOperationType.INSERT
            OperationType.UPDATE -> ChangeOperationType.UPDATE
            OperationType.REPLACE -> ChangeOperationType.REPLACE
            OperationType.DELETE -> ChangeOperationType.DELETE
            OperationType.DROP -> ChangeOperationType.DROP
            OperationType.RENAME -> ChangeOperationType.RENAME
            OperationType.DROP_DATABASE -> ChangeOperationType.DROP_DATABASE
            OperationType.INVALIDATE -> ChangeOperationType.INVALIDATE
            else -> {
                logger.warn("Unknown MongoDB operation type: $mongoOperationType, mapping to UPDATE")
                ChangeOperationType.UPDATE
            }
        }
    }

    /**
     * Processes a single change event from the change stream with enhanced error handling.
     * This method is called by the event processor with timeout protection.
     */
    private suspend fun processChangeEventSafely(change: ChangeStreamDocument<D>) {
        try {
            when (change.operationType) {
                OperationType.INSERT, OperationType.REPLACE, OperationType.UPDATE -> {
                    val fullDoc = change.fullDocument
                    if (fullDoc != null) {
                        eventHandler.onDocumentChanged(fullDoc, mapOperationType(change.operationType!!))
                        logger.debug("Processed ${change.operationType} for document: ${fullDoc.key}")
                    } else {
                        logger.warn(
                            "No fullDocument for ${change.operationType} operation " +
                                "in ${collection.namespace.collectionName}"
                        )
                    }
                }
                OperationType.DELETE -> {
                    val documentKey = change.documentKey
                    if (documentKey != null) {
                        val id = extractIdFromDocumentKey(documentKey)
                        if (id != null) {
                            try {
                                @Suppress("UNCHECKED_CAST")
                                eventHandler.onDocumentDeleted(id as K)
                                logger.debug("Processed DELETE for document ID: $id")
                            } catch (_: ClassCastException) {
                                logger.error(
                                    "Type mismatch in document ID during DELETE processing: " +
                                        "expected type K, got ${id::class.simpleName}"
                                )
                            }
                        } else {
                            logger.warn(
                                "Could not extract ID from delete operation " +
                                    "in ${collection.namespace.collectionName}"
                            )
                        }
                    }
                }
                else -> {
                    logger.debug(
                        "Ignored change stream operation: ${change.operationType} " +
                            "for ${collection.namespace.collectionName}"
                    )
                }
            }
        } catch (e: Exception) {
            logger.error(e, "Error processing change event for ${collection.namespace.collectionName}")
            // Don't rethrow - we want to continue processing other events
        }
    }

    /**
     * Extracts the document ID from a change stream document key.
     */
    private fun extractIdFromDocumentKey(documentKey: BsonDocument): Any? {
        return try {
            val bsonValue = documentKey["_id"]
            when {
                bsonValue?.isObjectId == true -> bsonValue.asObjectId().value.toHexString()
                bsonValue?.isString == true -> bsonValue.asString().value
                bsonValue?.isInt32 == true -> bsonValue.asInt32().value
                bsonValue?.isInt64 == true -> bsonValue.asInt64().value
                else -> {
                    logger.warn("Unsupported ID type in document key: ${bsonValue?.bsonType}")
                    null
                }
            }
        } catch (e: Exception) {
            logger.error(e, "Error extracting ID from document key")
            null
        }
    }
}