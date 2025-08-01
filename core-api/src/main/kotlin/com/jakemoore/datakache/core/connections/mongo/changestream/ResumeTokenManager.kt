package com.jakemoore.datakache.core.connections.mongo.changestream

import com.jakemoore.datakache.api.doc.Doc
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.FullDocument
import kotlinx.coroutines.flow.Flow
import org.bson.BsonDocument
import org.bson.BsonTimestamp

/**
 * Manages resume tokens and change stream configuration.
 * Handles the fallback chain: resumeToken → lastResumeToken → effectiveStartTime → current time
 */
internal class ResumeTokenManager<K : Any, D : Doc<K, D>>(
    private val context: ChangeStreamContext<K, D>,
    private val errorHandler: ChangeStreamErrorHandler<K, D>
) {

    // Resume token management with proper fallback chain
    private var resumeToken: BsonDocument? = null
    private var lastResumeToken: BsonDocument? = null
    private var effectiveStartTime: BsonTimestamp? = null

    /**
     * Sets the effective start time for the change stream.
     */
    fun setEffectiveStartTime(startTime: BsonTimestamp?) {
        this.effectiveStartTime = startTime
        context.logger.debug(
            "Set effective start time: $startTime"
        )
    }

    /**
     * Updates resume tokens after successful event processing.
     */
    fun updateTokens(newResumeToken: BsonDocument?) {
        lastResumeToken = resumeToken
        resumeToken = newResumeToken
    }

    /**
     * Clears only resume tokens, preserving other state.
     */
    fun clearTokensOnly() {
        resumeToken = null
        lastResumeToken = null
        context.logger.debug(
            "Cleared resume tokens."
        )
    }

    /**
     * Clears all tokens including effective start time.
     */
    @Suppress("unused")
    fun clearAllTokens() {
        resumeToken = null
        lastResumeToken = null
        effectiveStartTime = null
        context.logger.debug(
            "Cleared all tokens."
        )
    }

    /**
     * Handles resume token errors and clears tokens if necessary.
     */
    fun handleTokenError(e: Exception) {
        if (errorHandler.handleResumeTokenError(e)) {
            clearTokensOnly()
        }
    }

    /**
     * Helper function to try configuring the change stream with a specific fallback option.
     *
     * @param description Human-readable description of the configuration being attempted
     * @param configAction Lambda that performs the actual configuration
     * @param handleResumeErrors Whether to handle resume token specific errors (default: true)
     * @return true if configuration succeeded, false otherwise
     */
    private fun tryConfigureStream(
        handleResumeErrors: Boolean = true,
        description: String,
        configAction: () -> Unit
    ): Boolean {
        return try {
            configAction()
            context.logger.debug(description)
            true
        } catch (e: Exception) {
            context.logger.warn("$description failed: ${e.message}")
            if (handleResumeErrors) {
                handleSpecificResumeTokenError(e)
            }
            false
        }
    }

    /**
     * Configures the MongoDB change stream with proper settings and enhanced fallback chain.
     * Implements: resumeToken → lastResumeToken → effectiveStartTime → current time
     */
    @Suppress("KotlinConstantConditions")
    fun configureChangeStream(): Flow<ChangeStreamDocument<D>> {
        val watchBuilder = context.collection.watch(pipeline = emptyList<BsonDocument>()).apply {
            try {
                // Always enable full document retrieval for UPDATE operations (improves cache accuracy)
                fullDocument(FullDocument.UPDATE_LOOKUP)
            } catch (e: Exception) {
                context.logger.warn("Could not set fullDocument mode, proceeding without it: ${e.message}")
            }

            // Enhanced fallback chain for stream positioning
            var configured = false

            // First try: Current resume token
            val resumeToken = resumeToken
            if (!configured && resumeToken != null) {
                configured = tryConfigureStream(description = "Resuming change stream from current resume token") {
                    resumeAfter(resumeToken)
                }
            }

            // Second try: Last resume token fallback
            val lastResumeToken = lastResumeToken
            if (!configured && lastResumeToken != null) {
                configured = tryConfigureStream(description = "Resuming change stream from last resume token") {
                    resumeAfter(lastResumeToken)
                }
            }

            // Third try: Operation time fallback
            val effectiveStartTime = effectiveStartTime
            if (!configured && effectiveStartTime != null) {
                configured = tryConfigureStream(
                    description = "Starting change stream from operation time $effectiveStartTime",
                    configAction = { startAtOperationTime(effectiveStartTime) },
                    handleResumeErrors = false
                )
            }

            // Last resort: Current time
            if (!configured) {
                context.logger.warn(
                    "All fallback options failed, starting change stream from current time"
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
                context.logger.warn(
                    "Resume token invalidated due to specific error, clearing tokens: ${e.message}"
                )
                clearTokensOnly()
            }

            // Network or temporary errors - keep tokens for retry
            errorMessage.contains("connection") ||
                errorMessage.contains("timeout") ||
                errorMessage.contains("network") -> {
                context.logger.warn(
                    "Network error with resume token, keeping tokens for retry: ${e.message}"
                )
            }

            // Unknown resume token error - be conservative and clear
            errorMessage.contains("resume") -> {
                context.logger.warn(
                    "Unknown resume token error, clearing tokens: ${e.message}"
                )
                clearTokensOnly()
            }

            else -> {
                context.logger.warn(
                    "General error with change stream configuration: ${e.message}"
                )
            }
        }
    }

    /**
     * Performs token cleanup and maintenance.
     * Should be called periodically during event processing.
     */
    fun performTokenMaintenance(eventsProcessed: Long) {
        // Clean up old tokens if we have successful processing
        if (eventsProcessed % 1000 == 0L && resumeToken != null) {
            lastResumeToken = null // Clear very old token
            context.logger.debug(
                "Performed token cleanup"
            )
        }
    }

    /**
     * Gets the current resume token.
     */
    @Suppress("unused")
    fun getCurrentResumeToken(): BsonDocument? = resumeToken

    /**
     * Gets the last resume token.
     */
    @Suppress("unused")
    fun getLastResumeToken(): BsonDocument? = lastResumeToken

    /**
     * Gets the effective start time.
     */
    @Suppress("unused")
    fun getEffectiveStartTime(): BsonTimestamp? = effectiveStartTime

    /**
     * Checks if any tokens are available.
     */
    @Suppress("unused")
    fun hasTokens(): Boolean = resumeToken != null || lastResumeToken != null || effectiveStartTime != null
}
