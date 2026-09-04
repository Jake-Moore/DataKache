package com.jakemoore.datakache.core.connections.mongo.changestream

import com.jakemoore.datakache.api.changes.ChangeDocumentType
import com.jakemoore.datakache.api.changes.ChangeOperationType
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.metrics.ChangeStreamQueueStats
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.ordering.OperationTime
import com.jakemoore.datakache.core.connections.changes.ChangeStreamState
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.OperationType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeout
import org.bson.BsonDocument
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.TimeSource

/**
 * Handles event processing for change streams including channel management,
 * backpressure handling, and individual event processing logic.
 */
internal class ChangeStreamEventProcessor<K : Any, D : Doc<K, D>>(
    private val context: ChangeStreamContext<K, D>,
    private val stateManager: ChangeStreamStateManager<K, D>,
    private val errorHandler: ChangeStreamErrorHandler<K, D>,
    private val resumeTokenManager: ResumeTokenManager<K, D>,
) {
    // Event processing with backpressure - recreated in start()
    private var eventChannel: Channel<StreamItem<D>>? = null

    // For monitoring and debugging
    private var totalEventsProcessed = 0L
    private var lastTokenCleanupTime = TimeSource.Monotonic.markNow()

    /**
     * How many items are waiting in the buffer, and the deepest it has been since the last snapshot
     * was taken. Almost all are events; a reposition marker is at most one per reconnect.
     *
     * Tracked rather than read from the channel, which exposes no size. Two atomics per event is a
     * price worth paying to make buffer pressure observable: a full buffer pauses the stream, so
     * depth approaching capacity is the cache falling behind the database, and it is otherwise
     * invisible until it is already a problem.
     */
    private val queueDepth = AtomicInteger(0)

    private val queuePeak = AtomicInteger(0)

    /** Never reset, so a second poller cannot silently take the peak away from the first. */
    private val queuePeakAllTime = AtomicInteger(0)

    /** Resets [peakSinceLastRead][ChangeStreamQueueStats.peakSinceLastRead] as it reads it. */
    fun getQueueStats(): ChangeStreamQueueStats {
        // One read of the depth, so the reported value and the peak's new baseline are the same
        // instant. The three fields are still not one atomic snapshot, which a gauge does not need.
        val depth = queueDepth.get()
        return ChangeStreamQueueStats(
            capacity = context.config.maxBufferedEvents,
            depth = depth,
            peakSinceLastRead = queuePeak.getAndSet(depth),
            peakAllTime = queuePeakAllTime.get(),
        )
    }

    private fun recordEnqueued() {
        val depth = queueDepth.incrementAndGet()
        queuePeak.updateAndGet { peak -> if (depth > peak) depth else peak }
        queuePeakAllTime.updateAndGet { peak -> if (depth > peak) depth else peak }
    }

    /**
     * Creates a new event channel, replacing any existing one.
     * Critical for restart scenarios since closed channels cannot be reused.
     */
    fun createNewEventChannel() {
        eventChannel?.close() // Close existing channel if any
        eventChannel = Channel<StreamItem<D>>(capacity = context.config.maxBufferedEvents)
        queueDepth.set(0)
        queuePeak.set(0)
        // queuePeakAllTime deliberately survives: it says what this stream has ever reached, and a
        // reconnection replacing the channel is exactly when a reader most wants that to hold.
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
    fun startEventProcessing(scope: CoroutineScope): Job =
        scope.launch {
        context.logger.debug("Event processor started")

        try {
            while (stateManager.getCurrentState() != ChangeStreamState.SHUTDOWN) {
                try {
                    // Prevent excessive CPU usage with very small timeouts
                    val baseInterval = context.config.eventProcessingTimeout.inWholeMilliseconds / 10
                    val checkInterval = minOf(maxOf(baseInterval, 100), 5000) // Min 100ms, max 5s

                    val currentChannel = eventChannel
                    val item =
                        select {
                            if (currentChannel != null && !currentChannel.isClosedForReceive) {
                                currentChannel.onReceive { it }
                            }
                            onTimeout(checkInterval) { null }
                        }

                    if (item != null) {
                        queueDepth.decrementAndGet()
                        withTimeout(context.config.eventProcessingTimeout) {
                            when (item) {
                                // Taken from the buffer in order, so it lands exactly between the
                                // connection that produced the events before it and the one that
                                // produced those after.
                                is StreamItem.Reposition -> {
                                    context.eventHandler.onStreamRepositioned()
                                }

                                is StreamItem.Event -> {
                                    val event = item.change
                                    // Only on success. Advancing past an event the cache did not
                                    // apply means a later reconnection resumes after it, and the
                                    // mutation is never delivered again: a silent, permanent hole.
                                    // A later event that does succeed will move the position past
                                    // it anyway, which is the wider problem this does not solve,
                                    // but moving it for an event known to have failed is a choice
                                    // rather than a race.
                                    if (processChangeEventSafely(event)) {
                                        // The ordered path, so also the only place the
                                        // operation-time fallback may move.
                                        resumeTokenManager.updateTokens(event.resumeToken)
                                        resumeTokenManager.advanceEffectiveStartTime(event.clusterTime)
                                    }
                                }
                            }

                            totalEventsProcessed =
                                if (totalEventsProcessed >= Long.MAX_VALUE - 1) {
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
                        "Event processing timeout",
                    )
                } catch (_: CancellationException) {
                    context.logger.debug(
                        "Event processor cancelled",
                    )
                    break
                } catch (e: Exception) {
                    // Classify exceptions to determine if processor should continue
                    if (errorHandler.shouldEventProcessorStop(e)) {
                        context.logger.error(
                            e,
                            "Fatal error in event processor, stopping.",
                        )
                        break
                    } else {
                        context.logger.error(
                            e,
                            "Recoverable error in event processor",
                        )
                    }
                }
            }
        } finally {
            context.logger.debug("Event processor stopped")
        }
    }

    /**
     * Hands an event to the ordered buffer, waiting for room rather than jumping the queue.
     *
     * **A full buffer suspends the producer.** The alternative, applying the event immediately so as
     * not to drop it, is what this used to do, and it applied that event ahead of everything still
     * queued. The cache orders what it applies by the database's clock and treats commit order as a
     * guarantee, so a single event delivered out of order can leave a document permanently wrong
     * with no later event to repair it. **A dropped event is recoverable and an out-of-order event
     * is not**, which is the trade the old fallback had backwards.
     *
     * Suspending here only stops this coroutine pulling from the change stream cursor. MongoDB is
     * pull based and holds the position, so the stream resumes where it left off. A consumer slow
     * enough to outlast the oplog produces a resume error, which is handled, logged and retried,
     * rather than silent divergence.
     *
     * @return true if the event was buffered, false if the channel was gone or closed.
     */
    @OptIn(DelicateCoroutinesApi::class)
    suspend fun handleIncomingEvent(change: ChangeStreamDocument<D>): Boolean {
        val channel = eventChannel
        if (channel == null || channel.isClosedForSend) {
            // Only reachable while shutting down or before a channel exists. Dropping is correct:
            // the resume token has not advanced past this event, so a restart redelivers it.
            context.logger.warn(
                "Change stream event arrived with no open buffer, operation: ${change.operationType}. " +
                    "It will be redelivered when the stream restarts.",
            )
            return false
        }

        return enqueue(channel, StreamItem.Event(change), describe = "${change.operationType}")
    }

    /**
     * Announces a reposition through the buffer, so it lands between the events it separates.
     *
     * **Never dropped, unlike an event.** A dropped event is redelivered, because the resume token
     * has not advanced past it. A reposition is synthetic, carries no token, and nothing regenerates
     * it: losing one leaves the cache believing the new connection continues the old one's progress,
     * for the life of the process, which makes the connection check vacuous.
     *
     * So with no buffer to put it in, it goes straight to the handler. That gives up the ordering
     * this exists for, but only in the direction that costs nothing: taking effect too early leaves
     * entries from before it forgettable only by the ceiling, which is conservative, while not
     * taking effect at all lets a replayed event apply over a forgotten position.
     */
    @OptIn(DelicateCoroutinesApi::class)
    suspend fun handleReposition(): Boolean {
        val channel = eventChannel
        if (channel == null || channel.isClosedForSend) {
            context.logger.warn(
                "Stream repositioned with no open buffer, applying it directly. Ordering against " +
                    "any events still in flight is given up, in the conservative direction.",
            )
            context.eventHandler.onStreamRepositioned()
            return true
        }
        return enqueue(channel, StreamItem.Reposition(), describe = "reposition")
    }

    @OptIn(DelicateCoroutinesApi::class)
    private suspend fun enqueue(channel: Channel<StreamItem<D>>, item: StreamItem<D>, describe: String): Boolean {
        // Counted BEFORE the item can be seen by the consumer. Counting after the send lets the
        // consumer dequeue and decrement first, which reads as a negative depth and hides the very
        // burst the peak exists to record.
        recordEnqueued()
        return try {
            // Fast path first, purely so a full buffer can be reported before we wait on it.
            if (channel.trySend(item).isSuccess) return true

            // trySend also fails on a closed channel, where "full" would be a lie and the send below
            // throws immediately.
            if (channel.isClosedForSend) throw ClosedSendChannelException("channel closed")

            context.logger.warn(
                "Change stream buffer full (${context.config.maxBufferedEvents} items) while " +
                    "queueing $describe, pausing the stream until it drains",
            )
            DataKacheMetrics.getReceiversInternal().forEach {
                it.onChangeStreamBackpressure(context.collection.namespace.collectionName)
            }

            channel.send(item)
            true
        } catch (_: ClosedSendChannelException) {
            queueDepth.decrementAndGet()
            context.logger.debug("Channel closed, stopping event processing")
            false
        }
    }

    /**
     * Core event processing logic shared between normal processing and event loss recovery.
     * @param change The change stream document to process
     * @return true if processing succeeded, false if it failed
     */
    private suspend fun processEventCore(change: ChangeStreamDocument<D>): Boolean {
        // Every event reaches the cache through the ordered buffer, so nothing is applied ahead of
        // anything else. The handler still takes outOfBand, and it is still passed explicitly,
        // because the cache's ordering rests on that promise: reintroducing a bypass has to be a
        // visible change here rather than a silent one.
        val operationType =
            mapOperationType(
                requireNotNull(change.operationType) {
                    $$"ChangeStreamDocument operationType cannot be null! Are you using $changeStreamSplitLargeEvent ?"
                },
            )

        when (operationType) {
            ChangeOperationType.INSERT, ChangeOperationType.REPLACE, ChangeOperationType.UPDATE -> {
                val fullDoc = change.fullDocument
                if (fullDoc != null) {
                    val changeType = ChangeDocumentType.fromOperationType(operationType)
                    context.eventHandler.onDocumentChanged(
                        fullDoc,
                        changeType,
                        change.eventOperationTime(),
                        outOfBand = false,
                    )
                    context.logger.debug("Processed $operationType for document: ${fullDoc.key}")
                    return true
                } else {
                    val message = "No fullDocument for $operationType operation"
                    context.logger.error(message)
                    return false
                }
            }

            ChangeOperationType.DELETE -> {
                val documentKey = change.documentKey
                if (documentKey != null) {
                    val keyString = extractIdFromDocumentKey(documentKey)
                    if (keyString != null) {
                        context.eventHandler.onDocumentDeleted(
                            keyString,
                            change.eventOperationTime(),
                            outOfBand = false,
                        )
                        context.logger.debug("Processed DELETE for document: $keyString")
                        return true
                    } else {
                        val message = "Could not extract ID from delete operation"
                        context.logger.warn(message)
                        return false
                    }
                }
                return false
            }

            ChangeOperationType.DROP -> {
                context.eventHandler.onCollectionDropped()
                context.logger.debug("Processed DROP operation")
                return true
            }

            ChangeOperationType.RENAME -> {
                context.eventHandler.onCollectionRenamed()
                context.logger.debug("Processed RENAME operation")
                return true
            }

            ChangeOperationType.DROP_DATABASE -> {
                context.eventHandler.onDatabaseDropped()
                context.logger.debug("Processed DROP_DATABASE operation")
                return true
            }

            ChangeOperationType.INVALIDATE -> {
                context.eventHandler.onChangeStreamInvalidated()
                context.logger.debug("Processed INVALIDATE operation")
                return true
            }

            ChangeOperationType.UNKNOWN -> {
                context.eventHandler.onUnknownOperation()
                context.logger.debug("Processed UNKNOWN operation")
                return true
            }
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
     *
     * @return Whether the event was applied. False means the cache did not receive this mutation,
     * so the stream's resume position must not move past it.
     */
    private suspend fun processChangeEventSafely(change: ChangeStreamDocument<D>): Boolean =
        try {
        processEventCore(change)
    } catch (e: Exception) {
        context.logger.error(e, "Error processing change event")
        // Not rethrown: one bad event should not stop the stream for every other key.
        false
    }

    /**
     * Converts MongoDB's OperationType to our database-agnostic ChangeOperationType.
     */
    private fun mapOperationType(mongoOperationType: OperationType): ChangeOperationType =
        when (mongoOperationType) {
        OperationType.INSERT -> {
            ChangeOperationType.INSERT
        }

        OperationType.UPDATE -> {
            ChangeOperationType.UPDATE
        }

        OperationType.REPLACE -> {
            ChangeOperationType.REPLACE
        }

        OperationType.DELETE -> {
            ChangeOperationType.DELETE
        }

        OperationType.DROP -> {
            ChangeOperationType.DROP
        }

        OperationType.RENAME -> {
            ChangeOperationType.RENAME
        }

        OperationType.DROP_DATABASE -> {
            ChangeOperationType.DROP_DATABASE
        }

        OperationType.INVALIDATE -> {
            ChangeOperationType.INVALIDATE
        }

        OperationType.OTHER -> {
            // OTHER is used for mongodb operations that this driver does not recognize
            //   Must be resolved by upgrading the driver to a newer version
            context.logger.warn("Unknown MongoDB operation type: $mongoOperationType, mapping to UNKNOWN")
            ChangeOperationType.UNKNOWN
        }
    }

    /**
     * Extracts the document ID (as a [String]) from a change stream document key.
     */
    private fun extractIdFromDocumentKey(documentKey: BsonDocument): String? =
        try {
        val bsonValue = documentKey["_id"]
        when {
            bsonValue?.isObjectId == true -> {
                bsonValue.asObjectId().value.toHexString()
            }

            bsonValue?.isString == true -> {
                bsonValue.asString().value
            }

            bsonValue?.isInt32 == true -> {
                bsonValue.asInt32().value.toString()
            }

            bsonValue?.isInt64 == true -> {
                bsonValue.asInt64().value.toString()
            }

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
     * Gets the current event buffer for external access.
     */
    @Suppress("unused")
    fun getCurrentChannel(): Channel<StreamItem<D>>? = eventChannel
}

/**
 * The cluster time this event was committed at, which is the same clock a write's session reports.
 *
 * Falls back to [OperationTime.UNKNOWN] rather than throwing: an event with no cluster time is
 * treated as older than everything, so it cannot overwrite state that has one.
 */
private fun <D : Any> ChangeStreamDocument<D>.eventOperationTime(): OperationTime =
    this.clusterTime?.let { OperationTime(it.value) } ?: OperationTime.UNKNOWN
