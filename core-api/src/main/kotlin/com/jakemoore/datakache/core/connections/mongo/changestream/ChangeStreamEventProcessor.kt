package com.jakemoore.datakache.core.connections.mongo.changestream

import com.jakemoore.datakache.api.changes.ChangeDocumentType
import com.jakemoore.datakache.api.changes.ChangeOperationType
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.core.connections.changes.ChangeStreamState
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.OperationType
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
import kotlinx.coroutines.withTimeout
import org.bson.BsonDocument
import kotlin.time.TimeSource

/**
 * Handles event processing for change streams including channel management,
 * backpressure handling, and individual event processing logic.
 */
internal class ChangeStreamEventProcessor<K : Any, D : Doc<K, D>>(
    private val context: ChangeStreamContext<K, D>,
    private val stateManager: ChangeStreamStateManager<K, D>,
    private val errorHandler: ChangeStreamErrorHandler<K, D>,
    private val resumeTokenManager: ResumeTokenManager<K, D>
) {

    // Event processing with backpressure - recreated in start()
    private var eventChannel: Channel<ChangeStreamDocument<D>>? = null

    // For monitoring and debugging
    private var totalEventsProcessed = 0L
    private var lastTokenCleanupTime = TimeSource.Monotonic.markNow()

    /**
     * Creates a new event channel, replacing any existing one.
     * Critical for restart scenarios since closed channels cannot be reused.
     */
    fun createNewEventChannel() {
        eventChannel?.close() // Close existing channel if any
        eventChannel = Channel(capacity = context.config.maxBufferedEvents)
        context.logger.debug("Created new event channel with capacity ${context.config.maxBufferedEvents}")
    }

    /**
     * Resets counters and state for restart scenarios.
     */
    fun resetCountersForRestart() {
        // Reset events counter to prevent overflow and provide fresh start
        totalEventsProcessed = 0L
        lastTokenCleanupTime = TimeSource.Monotonic.markNow()
        context.logger.debug("Reset counters for restart")
    }

    /**
     * Starts the event processor that handles events from the channel with timeout and backpressure.
     */
    @OptIn(ExperimentalCoroutinesApi::class, DelicateCoroutinesApi::class)
    fun startEventProcessing(scope: kotlinx.coroutines.CoroutineScope): Job {
        return scope.launch {
            context.logger.debug("Event processor started")

            try {
                while (stateManager.getCurrentState() != ChangeStreamState.SHUTDOWN) {
                    try {
                        // Prevent excessive CPU usage with very small timeouts
                        val baseInterval = context.config.eventProcessingTimeout.inWholeMilliseconds / 10
                        val checkInterval = minOf(maxOf(baseInterval, 100), 5000) // Min 100ms, max 5s

                        val currentChannel = eventChannel
                        val event = select {
                            if (currentChannel != null && !currentChannel.isClosedForReceive) {
                                currentChannel.onReceive { it }
                            }
                            onTimeout(checkInterval) { null }
                        }

                        if (event != null) {
                            withTimeout(context.config.eventProcessingTimeout) {
                                processChangeEventSafely(event)

                                // Update resume tokens after successful processing
                                resumeTokenManager.updateTokens(event.resumeToken)

                                totalEventsProcessed = if (totalEventsProcessed >= Long.MAX_VALUE - 1) {
                                    context.logger.debug("Events counter approaching max value, resetting to 0")
                                    0L
                                } else {
                                    totalEventsProcessed + 1
                                }

                                // Periodic cleanup
                                performPeriodicMaintenance()
                            }
                        }
                    } catch (_: TimeoutCancellationException) {
                        context.logger.debug(
                            "Event processing timeout"
                        )
                    } catch (_: CancellationException) {
                        context.logger.debug(
                            "Event processor cancelled"
                        )
                        break
                    } catch (e: Exception) {
                        // Classify exceptions to determine if processor should continue
                        if (errorHandler.shouldEventProcessorStop(e)) {
                            context.logger.error(
                                e,
                                "Fatal error in event processor, stopping."
                            )
                            break
                        } else {
                            context.logger.error(
                                e,
                                "Recoverable error in event processor"
                            )
                        }
                    }
                }
            } finally {
                context.logger.debug("Event processor stopped")
            }
        }
    }

    /**
     * Handles incoming change stream events and applies backpressure strategies.
     * @param change The change stream document
     * @return true if the event was handled successfully, false if it was lost
     */
    @OptIn(DelicateCoroutinesApi::class)
    suspend fun handleIncomingEvent(change: ChangeStreamDocument<D>): Boolean {
        // Enhanced backpressure strategy with event loss prevention
        val channel = eventChannel
        if (channel != null && !channel.isClosedForSend) {
            val sendResult = channel.trySend(change)

            when {
                sendResult.isSuccess -> {
                    // Event sent successfully
                    return true
                }
                sendResult.isFailure -> {
                    val exception = sendResult.exceptionOrNull()
                    if (exception is kotlinx.coroutines.channels.ClosedSendChannelException) {
                        context.logger.debug("Channel closed, stopping event processing")
                        return false
                    }

                    // Channel is full - implement enhanced backpressure with fallback
                    context.logger.warn(
                        "Event channel full (${context.config.maxBufferedEvents} events), " +
                            "implementing backpressure strategy"
                    )

                    // Try a few times with short delays, then implement fallback
                    return handleBackpressure(change, channel)
                }
            }
        } else {
            context.logger.error(
                "Event lost from invalid channel, operation: ${change.operationType}, attempting fallback"
            )
            // Also handle this as a potential event loss scenario
            handleEventLoss(change)
            return false
        }
        return false
    }

    /**
     * Handles backpressure when the event channel is full.
     */
    @OptIn(DelicateCoroutinesApi::class)
    private suspend fun handleBackpressure(
        change: ChangeStreamDocument<D>,
        channel: Channel<ChangeStreamDocument<D>>
    ): Boolean {
        var retryCount = 0
        val maxRetries = 3

        while (retryCount < maxRetries && !channel.isClosedForSend) {
            delay(50) // Short delay to allow processing
            val retryResult = channel.trySend(change)
            if (retryResult.isSuccess) {
                return true
            }
            retryCount++
        }

        // Implement fallback strategy
        context.logger.error(
            "Event lost due to backpressure, operation: ${change.operationType}, attempting fallback"
        )
        handleEventLoss(change)
        return false
    }

    /**
     * Core event processing logic shared between normal processing and event loss recovery.
     * @param change The change stream document to process
     * @param isRecoveryMode Whether this is being called from event loss recovery
     * @return true if processing succeeded, false if it failed
     */
    private suspend fun processEventCore(change: ChangeStreamDocument<D>, isRecoveryMode: Boolean = false): Boolean {
        val operationType = mapOperationType(
            requireNotNull(change.operationType) {
                $$"ChangeStreamDocument operationType cannot be null! Are you using $changeStreamSplitLargeEvent ?"
            }
        )

        when (operationType) {
            ChangeOperationType.INSERT, ChangeOperationType.REPLACE, ChangeOperationType.UPDATE -> {
                val fullDoc = change.fullDocument
                if (fullDoc != null) {
                    val changeType = ChangeDocumentType.fromOperationType(operationType)
                    context.eventHandler.onDocumentChanged(fullDoc, changeType)
                    if (isRecoveryMode) {
                        context.logger.warn("Recovered from lost $operationType event for document: ${fullDoc.key}")
                    } else {
                        context.logger.debug("Processed $operationType for document: ${fullDoc.key}")
                    }
                    return true
                } else {
                    val message = if (isRecoveryMode) {
                        "Cannot recover from lost event - no fullDocument available"
                    } else {
                        "No fullDocument for $operationType operation"
                    }
                    context.logger.error(message)
                    return false
                }
            }
            ChangeOperationType.DELETE -> {
                val documentKey = change.documentKey
                if (documentKey != null) {
                    val id = extractIdFromDocumentKey(documentKey)
                    if (id != null) {
                        try {
                            @Suppress("UNCHECKED_CAST")
                            context.eventHandler.onDocumentDeleted(id as K)
                            if (isRecoveryMode) {
                                context.logger.warn("Recovered from lost DELETE event for document: $id")
                            } else {
                                context.logger.debug("Processed DELETE for document: $id")
                            }
                            return true
                        } catch (_: ClassCastException) {
                            context.logger.error(
                                "Type mismatch in document ID during DELETE " +
                                    "${if (isRecoveryMode) "recovery" else "processing"}: " +
                                    "expected type K, got ${id::class.simpleName}"
                            )
                            return false
                        }
                    } else {
                        val message = if (isRecoveryMode) {
                            "Could not extract ID from delete operation during recovery"
                        } else {
                            "Could not extract ID from delete operation"
                        }
                        context.logger.warn(message)
                        return false
                    }
                }
                return false
            }
            ChangeOperationType.DROP -> {
                context.eventHandler.onCollectionDropped()
                if (isRecoveryMode) {
                    context.logger.warn("Recovered from lost DROP event")
                } else {
                    context.logger.debug("Processed DROP operation")
                }
                return true
            }
            ChangeOperationType.RENAME -> {
                context.eventHandler.onCollectionRenamed()
                if (isRecoveryMode) {
                    context.logger.warn("Recovered from lost RENAME event")
                } else {
                    context.logger.debug("Processed RENAME operation")
                }
                return true
            }
            ChangeOperationType.DROP_DATABASE -> {
                context.eventHandler.onDatabaseDropped()
                if (isRecoveryMode) {
                    context.logger.warn("Recovered from lost DROP_DATABASE event")
                } else {
                    context.logger.debug("Processed DROP_DATABASE operation")
                }
                return true
            }
            ChangeOperationType.INVALIDATE -> {
                context.eventHandler.onChangeStreamInvalidated()
                if (isRecoveryMode) {
                    context.logger.warn("Recovered from lost INVALIDATE event")
                } else {
                    context.logger.debug("Processed INVALIDATE operation")
                }
                return true
            }
            ChangeOperationType.UNKNOWN -> {
                context.eventHandler.onUnknownOperation()
                if (isRecoveryMode) {
                    context.logger.warn("Recovered from lost UNKNOWN event")
                } else {
                    context.logger.debug("Processed UNKNOWN operation")
                }
                return true
            }
        }
    }

    /**
     * Handles event loss scenarios with fallback strategies.
     */
    private suspend fun handleEventLoss(change: ChangeStreamDocument<D>) {
        try {
            processEventCore(change, isRecoveryMode = true)
        } catch (e: Exception) {
            context.logger.error(
                e,
                "Failed to recover from lost event - cache consistency may be compromised"
            )
        }
    }

    /**
     * Performs periodic maintenance like token cleanup.
     */
    private fun performPeriodicMaintenance() {
        val elapsedMillis = lastTokenCleanupTime.elapsedNow().inWholeMilliseconds
        if (elapsedMillis > 300_000) { // Every 5 minutes
            // Delegate token maintenance to the resume token manager
            resumeTokenManager.performTokenMaintenance(totalEventsProcessed)
            lastTokenCleanupTime = TimeSource.Monotonic.markNow()
        }
    }

    /**
     * Processes a single change event from the change stream with enhanced error handling.
     * This method is called by the event processor with timeout protection.
     */
    private suspend fun processChangeEventSafely(change: ChangeStreamDocument<D>) {
        try {
            processEventCore(change, isRecoveryMode = false)
        } catch (e: Exception) {
            context.logger.error(e, "Error processing change event")
            // Don't rethrow - we want to continue processing other events
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
            OperationType.OTHER -> {
                // OTHER is used for mongodb operations that this driver does not recognize
                //   Must be resolved by upgrading the driver to a newer version
                context.logger.warn("Unknown MongoDB operation type: $mongoOperationType, mapping to UNKNOWN")
                ChangeOperationType.UNKNOWN
            }
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
                    context.logger.warn("Unsupported ID type in document key: ${bsonValue?.bsonType}")
                    null
                }
            }
        } catch (_: NoSuchElementException) {
            context.logger.error("Document key missing '_id' field")
            null
        } catch (e: ClassCastException) {
            context.logger.error("Type conversion error extracting ID: ${e.message}")
            null
        } catch (e: IllegalArgumentException) {
            context.logger.error(e, "Error extracting ID from document key")
            null
        } catch (e: Exception) {
            context.logger.error(e, "Unknown Error extracting ID from document key")
            null
        }
    }

    /**
     * Closes the event channel and cleans up resources.
     */
    fun cleanup() {
        val channel = eventChannel
        if (channel != null) {
            try {
                channel.close()
                context.logger.debug("Event channel closed")
            } catch (e: Exception) {
                context.logger.warn("Error closing event channel: ${e.message}")
            }
        }
        eventChannel = null
    }

    /**
     * Gets the current event channel for external access.
     */
    @Suppress("unused")
    fun getCurrentChannel(): Channel<ChangeStreamDocument<D>>? = eventChannel
}
