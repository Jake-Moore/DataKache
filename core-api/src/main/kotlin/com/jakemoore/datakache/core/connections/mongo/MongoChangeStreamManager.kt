package com.jakemoore.datakache.core.connections.mongo

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.core.connections.DatabaseScope
import com.jakemoore.datakache.core.connections.changes.ChangeEventHandler
import com.jakemoore.datakache.core.connections.changes.ChangeStreamConfig
import com.jakemoore.datakache.core.connections.changes.ChangeStreamManager
import com.jakemoore.datakache.core.connections.changes.ChangeStreamState
import com.jakemoore.datakache.core.connections.mongo.changestream.ChangeStreamContext
import com.jakemoore.datakache.core.connections.mongo.changestream.ChangeStreamErrorHandler
import com.jakemoore.datakache.core.connections.mongo.changestream.ChangeStreamEventProcessor
import com.jakemoore.datakache.core.connections.mongo.changestream.ChangeStreamStateManager
import com.jakemoore.datakache.core.connections.mongo.changestream.ResumeTokenManager
import com.jakemoore.datakache.core.connections.mongo.changestream.RetryDecision
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.kotlin.client.coroutine.MongoCollection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.launch
import org.bson.BsonTimestamp

/**
 * MongoDB implementation of ChangeStreamManager that orchestrates between specialized components
 * for state management, error handling, event processing, and resume token management.
 *
 * This class acts as a coordinator, maintaining the same public interface while delegating
 * responsibilities to focused components for better maintainability and testability.
 */
class MongoChangeStreamManager<K : Any, D : Doc<K, D>>(
    collection: MongoCollection<D>,
    eventHandler: ChangeEventHandler<K, D>,
    config: ChangeStreamConfig = ChangeStreamConfig.forMongoDB(),
    logger: LoggerService
) : ChangeStreamManager<K, D>, DatabaseScope {

    // Shared context for all components
    private val context = ChangeStreamContext(collection, eventHandler, config, logger)

    // Specialized components
    private val stateManager = ChangeStreamStateManager(context)
    private val errorHandler = ChangeStreamErrorHandler(context)
    private val resumeTokenManager = ResumeTokenManager(context, errorHandler)
    private val eventProcessor = ChangeStreamEventProcessor(context, stateManager, errorHandler, resumeTokenManager)

    /**
     * Starts the change stream listener. If a startAtOperationTime is provided,
     * the stream will start from that point to avoid missing changes.
     * This method is thread-safe and completes all setup before returning.
     */
    override suspend fun start(startAtOperationTime: Any?) {
        stateManager.withStateLock {
            if (stateManager.isActive()) {
                context.logger.warn(
                    "Cannot start change stream - already in state: ${stateManager.getCurrentState()}"
                )
                return@withStateLock
            }

            if (!stateManager.canStart()) {
                context.logger.warn(
                    "Cannot start change stream - already in state: ${stateManager.getCurrentState()}"
                )
                return@withStateLock
            }

            if (!stateManager.transitionTo(null, ChangeStreamState.CONNECTING)) {
                context.logger.error(
                    "Failed to transition to CONNECTING state - concurrent state change detected"
                )
                return@withStateLock
            }

            try {
                // Set the effective start time from the provided parameter (critical timing gap fix)
                resumeTokenManager.setEffectiveStartTime(startAtOperationTime as? BsonTimestamp)

                // Reset error tracking for restart
                errorHandler.resetFailures()

                // Recreate the event channel since closed channels cannot be reused
                eventProcessor.createNewEventChannel()

                // Reset counters and state for restart
                eventProcessor.resetCountersForRestart()

                // Start the event processor first
                val processorJob = eventProcessor.startEventProcessing(this@MongoChangeStreamManager)

                // Then start the change stream watcher
                val streamJob = this@MongoChangeStreamManager.launch {
                    try {
                        startChangeStreamWithRetry()
                    } catch (e: Exception) {
                        context.logger.error(
                            e,
                            "Fatal error in change stream"
                        )
                        stateManager.transitionTo(null, ChangeStreamState.FAILED)
                        stateManager.clearJobsUnsafe()
                        throw e
                    }
                }

                // Register jobs with state manager
                stateManager.setJobs(streamJob, processorJob)

                context.logger.debug("Change stream jobs started")
            } catch (e: Exception) {
                context.logger.error(
                    e,
                    "Failed to start change stream"
                )
                stateManager.transitionTo(null, ChangeStreamState.FAILED)
                stateManager.clearJobsUnsafe()
                throw e
            }
        }
    }

    /**
     * Stops the change stream listener.
     * This method is thread-safe and completes all cleanup before returning.
     */
    override suspend fun stop() {
        stateManager.withStateLock {
            val currentState = stateManager.getCurrentState()
            if (currentState == ChangeStreamState.SHUTDOWN) {
                context.logger.debug(
                    "Change stream already shutdown"
                )
                return@withStateLock // Already shutdown
            }

            // Transition to shut down state
            if (!stateManager.transitionTo(currentState, ChangeStreamState.SHUTDOWN)) {
                context.logger.warn(
                    "Failed to transition to SHUTDOWN state " +
                        "- attempted from: $currentState, current state: ${stateManager.getCurrentState()}"
                )
                return@withStateLock
            }

            context.logger.debug("Shutting down change stream")

            // Proper resource cleanup with job completion waiting
            cleanupResourcesWithJobCompletion()

            context.logger.debug("Change stream shutdown completed")
        }
    }

    /**
     * Proper resource cleanup that waits for jobs to complete before cleaning up.
     * This method should only be called while holding the state lock.
     */
    private suspend fun cleanupResourcesWithJobCompletion() {
        context.logger.debug("Starting resource cleanup")

        try {
            // Cancel jobs and wait for completion
            stateManager.cancelJobs()
            context.logger.debug("Jobs completed")

            // Clean up event processor resources
            eventProcessor.cleanup()
            context.logger.debug("Event processor cleaned up")
        } catch (e: Exception) {
            if (errorHandler.isCriticalCleanupError(e)) {
                context.logger.error(e, "Critical error during cleanup")
                throw e
            } else {
                context.logger.warn("Non-critical cleanup error: ${e.message}")
            }
        }

        context.logger.debug("Resource cleanup completed")
    }

    /**
     * Main change stream loop with retry logic (orchestrator method).
     */
    private suspend fun startChangeStreamWithRetry() {
        var retryCount = 0

        while (stateManager.getCurrentState() != ChangeStreamState.SHUTDOWN && retryCount < context.config.maxRetries) {
            try {
                if (!stateManager.transitionTo(
                        stateManager.getCurrentState(),
                        ChangeStreamState.CONNECTING
                    )
                ) {
                    context.logger.warn("Failed to transition to CONNECTING state")
                    break
                }

                context.logger.debug(
                    "Starting change stream (attempt ${retryCount + 1})"
                )

                val watchFlow = resumeTokenManager.configureChangeStream()
                processChangeStreamEvents(watchFlow)

                // If we reach here, the stream ended normally (which shouldn't happen)
                context.logger.warn("Change stream ended normally")
                break
            } catch (_: CancellationException) {
                context.logger.debug("Change stream cancelled")
                break
            } catch (e: Exception) {
                val decision = errorHandler.handleError(e, retryCount)
                when (decision) {
                    is RetryDecision.Stop -> break
                    is RetryDecision.StopWithError -> {
                        stateManager.transitionTo(null, ChangeStreamState.FAILED)
                        break
                    }
                    is RetryDecision.Continue -> {
                        // Handle resume token errors
                        if (errorHandler.isResumeTokenError(e)) {
                            resumeTokenManager.handleTokenError(e)
                        }

                        // Notify handler of disconnection
                        if (stateManager.getCurrentState() == ChangeStreamState.CONNECTED) {
                            context.eventHandler.onDisconnected()
                        }

                        stateManager.transitionTo(null, ChangeStreamState.RECONNECTING)
                        retryCount++
                    }
                }
            }
        }

        if (retryCount >= context.config.maxRetries) {
            stateManager.transitionTo(null, ChangeStreamState.FAILED)
            context.logger.error(
                "Change stream failed permanently after $retryCount attempts"
            )
        } else if (stateManager.getCurrentState() != ChangeStreamState.SHUTDOWN) {
            stateManager.transitionTo(null, ChangeStreamState.DISCONNECTED)
        }
    }

    /**
     * Processes change stream events from the MongoDB watch flow.
     */
    private suspend fun processChangeStreamEvents(watchFlow: kotlinx.coroutines.flow.Flow<ChangeStreamDocument<D>>) {
        watchFlow.collect { change ->
            if (stateManager.getCurrentState() == ChangeStreamState.SHUTDOWN) {
                return@collect // Exit if shutdown was requested
            }

            // Update state to connected on first successful event
            if (stateManager.transitionTo(ChangeStreamState.CONNECTING, ChangeStreamState.CONNECTED) ||
                stateManager.transitionTo(ChangeStreamState.RECONNECTING, ChangeStreamState.CONNECTED)
            ) {
                onSuccessfulConnection()
            }

            // Handle the event through the event processor (tokens updated after processing)
            eventProcessor.handleIncomingEvent(change)
        }
    }

    /**
     * Handles successful connection to the change stream.
     */
    private suspend fun onSuccessfulConnection() {
        if (errorHandler.getConsecutiveFailures() > 0) {
            context.logger.debug(
                "Change stream reconnected after ${errorHandler.getConsecutiveFailures()} failures"
            )
        }

        errorHandler.resetFailures()
        context.eventHandler.onConnected()
        context.logger.debug("Change stream connected")
    }

    // Public interface methods (delegating to components)

    /**
     * Gets the current state of the change stream.
     */
    override fun getCurrentState(): ChangeStreamState = stateManager.getCurrentState()

    /**
     * Gets the last error that occurred, if any.
     */
    override fun getLastError(): Throwable? = errorHandler.getLastError()

    /**
     * Gets the number of consecutive failures.
     */
    override fun getConsecutiveFailures(): Int = errorHandler.getConsecutiveFailures()

    override fun areJobsActive(): Boolean {
        return stateManager.areJobsActive()
    }
}
