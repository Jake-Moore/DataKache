@file:Suppress("unused")

package com.jakemoore.datakache.core.connections.mongo.changestream

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.core.connections.changes.ChangeOperationType
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
    private var lastTokenCleanupTime = System.currentTimeMillis()

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
        lastTokenCleanupTime = System.currentTimeMillis()
        context.logger.debug("Reset counters for restart")
    }

    /**
     * Starts the event processor that handles events from the channel with timeout and backpressure.
     */
    @OptIn(ExperimentalCoroutinesApi::class, DelicateCoroutinesApi::class)
    fun startEventProcessing(scope: kotlinx.coroutines.CoroutineScope): Job {
        return scope.launch {
            context.logger.debug("Event processor started for ${context.collection.namespace.collectionName}")

            try {
                while (stateManager.getCurrentState() != ChangeStreamState.SHUTDOWN) {
                    try {
                        // CRITICAL FIX: Prevent excessive CPU usage with very small timeouts
                        val baseInterval = context.config.eventProcessingTimeout.inWholeMilliseconds / 10
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
                            withTimeout(context.config.eventProcessingTimeout) {
                                processChangeEventSafely(event)

                                // CRITICAL FIX: Update resume tokens after successful processing
                                resumeTokenManager.updateTokens(event.resumeToken)

                                // CRITICAL FIX: Better overflow protection using >= instead of ==
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
                            "Event processing timeout for ${context.collection.namespace.collectionName}"
                        )
                    } catch (_: CancellationException) {
                        context.logger.debug(
                            "Event processor cancelled for ${context.collection.namespace.collectionName}"
                        )
                        break
                    } catch (e: Exception) {
                        // Classify exceptions to determine if processor should continue
                        if (errorHandler.shouldEventProcessorStop(e)) {
                            context.logger.error(
                                e,
                                "Fatal error in event processor, " +
                                    "stopping: ${context.collection.namespace.collectionName}"
                            )
                            break
                        } else {
                            context.logger.error(
                                e,
                                "Recoverable error in event processor " +
                                    "for ${context.collection.namespace.collectionName}"
                            )
                        }
                    }
                }
            } finally {
                context.logger.debug("Event processor stopped for ${context.collection.namespace.collectionName}")
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
        // CRITICAL FIX: Enhanced backpressure strategy with event loss prevention
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
                            "implementing backpressure strategy for ${context.collection.namespace.collectionName}"
                    )

                    // Strategy: Try a few times with short delays, then implement fallback
                    return handleBackpressure(change, channel)
                }
            }
        } else {
            context.logger.debug("Event channel unavailable, skipping event")
            // Also handle this as a potential event loss scenario
            handleEventLossScenario(change)
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

        // CRITICAL FIX: Implement fallback strategy instead of losing events
        handleEventLossScenario(change)
        return false
    }

    /**
     * Handles event loss scenarios with fallback strategies.
     */
    private suspend fun handleEventLossScenario(change: ChangeStreamDocument<D>) {
        context.logger.error(
            "Event lost due to backpressure for ${context.collection.namespace.collectionName}, " +
                "operation: ${change.operationType}, implementing fallback strategy"
        )

        // CRITICAL FIX: Implement fallback strategies instead of just logging
        try {
            when (change.operationType) {
                OperationType.INSERT, OperationType.UPDATE, OperationType.REPLACE -> {
                    // For data changes, we could trigger a selective cache refresh
                    val doc = change.fullDocument
                    if (doc != null) {
                        context.logger.warn("Triggering direct cache update for lost event: ${doc.key}")
                        // Process the event directly to maintain consistency
                        processChangeEventSafely(change)
                    } else {
                        context.logger.error("Cannot recover from lost event - no fullDocument available")
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
                                context.eventHandler.onDocumentDeleted(id as K)
                                context.logger.warn("Recovered from lost DELETE event for document: $id")
                            } catch (_: ClassCastException) {
                                context.logger.error(
                                    "Type mismatch in document ID during DELETE recovery: " +
                                        "expected type K, got ${id::class.simpleName}"
                                )
                            }
                        }
                    }
                }
                else -> {
                    context.logger.error("Cannot recover from lost event of type: ${change.operationType}")
                }
            }
        } catch (e: Exception) {
            context.logger.error(e, "Failed to recover from lost event - cache consistency may be compromised")
        }
    }

    /**
     * Performs periodic maintenance like token cleanup.
     */
    private fun performPeriodicMaintenance() {
        val now = System.currentTimeMillis()
        if (now - lastTokenCleanupTime > 300_000) { // Every 5 minutes
            // Delegate token maintenance to the resume token manager
            resumeTokenManager.performTokenMaintenance(totalEventsProcessed)
            lastTokenCleanupTime = now
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
                        context.eventHandler.onDocumentChanged(fullDoc, mapOperationType(change.operationType!!))
                        context.logger.debug("Processed ${change.operationType} for document: ${fullDoc.key}")
                    } else {
                        context.logger.warn(
                            "No fullDocument for ${change.operationType} operation " +
                                "in ${context.collection.namespace.collectionName}"
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
                                context.eventHandler.onDocumentDeleted(id as K)
                                context.logger.debug("Processed DELETE for document ID: $id")
                            } catch (_: ClassCastException) {
                                context.logger.error(
                                    "Type mismatch in document ID during DELETE processing: " +
                                        "expected type K, got ${id::class.simpleName}"
                                )
                            }
                        } else {
                            context.logger.warn(
                                "Could not extract ID from delete operation " +
                                    "in ${context.collection.namespace.collectionName}"
                            )
                        }
                    }
                }
                else -> {
                    context.logger.debug(
                        "Ignored change stream operation: ${change.operationType} " +
                            "for ${context.collection.namespace.collectionName}"
                    )
                }
            }
        } catch (e: Exception) {
            context.logger.error(e, "Error processing change event for ${context.collection.namespace.collectionName}")
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
            else -> {
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
        } catch (e: Exception) {
            context.logger.error(e, "Error extracting ID from document key")
            null
        }
    }

    /**
     * Closes the event channel and cleans up resources.
     */
    @Suppress("RedundantSuspendModifier")
    suspend fun cleanup() {
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
    fun getCurrentChannel(): Channel<ChangeStreamDocument<D>>? = eventChannel
}
