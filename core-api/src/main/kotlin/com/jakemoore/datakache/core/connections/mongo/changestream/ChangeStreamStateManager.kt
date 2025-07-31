package com.jakemoore.datakache.core.connections.mongo.changestream

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.core.connections.changes.ChangeStreamState
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicReference

/**
 * Manages the state and lifecycle of change stream connections.
 * Handles thread-safe state transitions and job lifecycle management.
 */
internal class ChangeStreamStateManager<K : Any, D : Doc<K, D>>(
    private val context: ChangeStreamContext<K, D>
) {

    private val state = AtomicReference(ChangeStreamState.DISCONNECTED)
    private val stateMutex = Mutex()

    // Job references for lifecycle management
    private var changeStreamJob: Job? = null
    private var eventProcessorJob: Job? = null

    /**
     * Gets the current state of the change stream.
     */
    fun getCurrentState(): ChangeStreamState = state.get()

    /**
     * Attempts to transition to a new state atomically.
     * @param expectedState The expected current state (null to skip validation)
     * @param newState The new state to transition to
     * @return true if the transition was successful, false otherwise
     */
    fun transitionTo(expectedState: ChangeStreamState?, newState: ChangeStreamState): Boolean {
        return if (expectedState != null) {
            state.compareAndSet(expectedState, newState)
        } else {
            state.set(newState)
            true
        }
    }

    /**
     * Checks if the current state allows starting the change stream.
     */
    fun canStart(): Boolean {
        val currentState = state.get()
        return when (currentState) {
            ChangeStreamState.DISCONNECTED,
            ChangeStreamState.FAILED,
            ChangeStreamState.SHUTDOWN -> true
            else -> false
        }
    }

    /**
     * Checks if the current state indicates the stream is active.
     */
    fun isActive(): Boolean {
        val currentState = state.get()
        return when (currentState) {
            ChangeStreamState.CONNECTED,
            ChangeStreamState.CONNECTING,
            ChangeStreamState.RECONNECTING -> true
            else -> false
        }
    }

    /**
     * Executes an action while holding the state lock.
     * This ensures thread-safe state management operations.
     */
    suspend fun <T> withStateLock(action: suspend () -> T): T {
        return stateMutex.withLock { action() }
    }

    /**
     * Sets the job references for lifecycle management.
     */
    fun setJobs(streamJob: Job?, processorJob: Job?) {
        this.changeStreamJob = streamJob
        this.eventProcessorJob = processorJob
    }

    /**
     * Cancels and clears job references.
     */
    suspend fun cancelJobs() {
        changeStreamJob?.cancel()
        eventProcessorJob?.cancel()

        // Wait for jobs to complete
        try {
            changeStreamJob?.join()
            eventProcessorJob?.join()
        } catch (_: CancellationException) {
            // Expected when jobs are cancelled - can be safely ignored
        } catch (e: Exception) {
            context.logger.warn("Error waiting for job completion during cleanup: ${e.message}")
        }

        changeStreamJob = null
        eventProcessorJob = null
    }

    /**
     * Clears job references without cancelling (for emergency cleanup).
     */
    fun clearJobsUnsafe() {
        // UNSAFE: Does not wait for job completion, may leave jobs running
        // Only use during emergency shutdown or when jobs are known to be terminated
        val streamJob = changeStreamJob
        val processorJob = eventProcessorJob
        changeStreamJob = null
        eventProcessorJob = null
        streamJob?.cancel()
        processorJob?.cancel()
    }

    /**
     * Validates if a state transition is allowed.
     */
    fun isValidTransition(from: ChangeStreamState, to: ChangeStreamState): Boolean {
        return when (from) {
            ChangeStreamState.DISCONNECTED -> to in setOf(
                ChangeStreamState.CONNECTING,
                ChangeStreamState.SHUTDOWN
            )
            ChangeStreamState.CONNECTING -> to in setOf(
                ChangeStreamState.CONNECTED,
                ChangeStreamState.FAILED,
                ChangeStreamState.SHUTDOWN
            )
            ChangeStreamState.CONNECTED -> to in setOf(
                ChangeStreamState.RECONNECTING,
                ChangeStreamState.FAILED,
                ChangeStreamState.SHUTDOWN
            )
            ChangeStreamState.RECONNECTING -> to in setOf(
                ChangeStreamState.CONNECTED,
                ChangeStreamState.FAILED,
                ChangeStreamState.SHUTDOWN
            )
            ChangeStreamState.FAILED -> to in setOf(
                ChangeStreamState.CONNECTING,
                ChangeStreamState.SHUTDOWN
            )
            ChangeStreamState.SHUTDOWN -> false // No transitions from shutdown
        }
    }

    /**
     * Attempts a validated state transition.
     */
    @Suppress("unused")
    fun transitionToValidated(newState: ChangeStreamState): Boolean {
        val currentState = state.get()
        return if (isValidTransition(currentState, newState)) {
            state.compareAndSet(currentState, newState)
        } else {
            context.logger.warn(
                "Invalid state transition attempted from $currentState to $newState " +
                    "for ${context.collection.namespace.collectionName}"
            )
            false
        }
    }
}
