package com.jakemoore.datakache.core.connections.mongo.changestream

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.core.connections.changes.ChangeStreamConfig
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlin.math.min
import kotlin.math.pow

/**
 * Represents the decision after handling an error.
 */
internal sealed class RetryDecision {
    object Continue : RetryDecision()
    object Stop : RetryDecision()
    data class StopWithError(val error: Exception) : RetryDecision()
}

/**
 * Handles error classification, tracking, and retry logic for change streams.
 * Provides granular error analysis and appropriate recovery strategies.
 */
internal class ChangeStreamErrorHandler<K : Any, D : Doc<K, D>>(
    private val context: ChangeStreamContext<K, D>
) {

    // Error tracking
    private var consecutiveFailures = 0
    private var lastError: Throwable? = null

    /**
     * Gets the number of consecutive failures.
     */
    fun getConsecutiveFailures(): Int = consecutiveFailures

    /**
     * Gets the last error that occurred.
     */
    fun getLastError(): Throwable? = lastError

    /**
     * Increments the failure count and records the error.
     */
    fun recordFailure(error: Throwable): Int {
        consecutiveFailures++
        lastError = error
        return consecutiveFailures
    }

    /**
     * Resets the failure count and clears the last error.
     */
    fun resetFailures() {
        consecutiveFailures = 0
        lastError = null
    }

    /**
     * Determines if an error is fatal and should stop all retry attempts.
     * Enhanced with more nuanced error classification.
     */
    fun isFatalError(e: Exception): Boolean {
        val errorMessage = e.message?.lowercase() ?: ""

        return when {
            // Definite fatal errors - authentication/authorization
            errorMessage.contains("authentication failed") ||
                errorMessage.contains("authorization failed") ||
                errorMessage.contains("access denied") ||
                errorMessage.contains("unauthorized") -> {
                context.logger.error("Fatal authentication/authorization error: ${e.message}")
                true
            }

            // MongoDB version/feature incompatibility
            errorMessage.contains("change streams are only supported") ||
                errorMessage.contains("feature is not supported") -> {
                context.logger.error("Fatal feature incompatibility error: ${e.message}")
                true
            }

            // Database/collection doesn't exist
            errorMessage.contains("database does not exist") ||
                errorMessage.contains("collection does not exist") -> {
                context.logger.error("Fatal resource not found error: ${e.message}")
                true
            }

            // Potentially recoverable errors that we used to consider fatal
            errorMessage.contains("connection") ||
                errorMessage.contains("timeout") ||
                errorMessage.contains("network") ||
                errorMessage.contains("host unreachable") -> {
                context.logger.warn("Network error, will retry: ${e.message}")
                false
            }

            else -> {
                // Unknown errors - be conservative but not fatal initially
                context.logger.warn("Unknown error, will retry with caution: ${e.message}")
                false
            }
        }
    }

    /**
     * Determines if an error is related to resume tokens.
     */
    fun isResumeTokenError(e: Exception): Boolean {
        val errorMessage = e.message?.lowercase() ?: ""

        return errorMessage.contains("resume point may no longer be in the oplog") ||
            errorMessage.contains("invalid resume point") ||
            (errorMessage.contains("resume token") && errorMessage.contains("invalid")) ||
            errorMessage.contains("resume")
    }

    /**
     * Handles specific resume token errors with granular error detection.
     */
    fun handleResumeTokenError(e: Exception): Boolean {
        val errorMessage = e.message?.lowercase() ?: ""

        return when {
            // Specific resume token errors that require token clearing
            errorMessage.contains("resume point may no longer be in the oplog") ||
                errorMessage.contains("invalid resume point") ||
                errorMessage.contains("resume token") && errorMessage.contains("invalid") -> {
                context.logger.warn("Resume token invalidated due to specific error, clearing tokens: ${e.message}")
                true // Indicates tokens should be cleared
            }

            // Network or temporary errors - keep tokens for retry
            errorMessage.contains("connection") ||
                errorMessage.contains("timeout") ||
                errorMessage.contains("network") -> {
                context.logger.warn("Network error with resume token, keeping tokens for retry: ${e.message}")
                false // Keep tokens
            }

            // Unknown resume token error - be conservative and clear
            errorMessage.contains("resume") -> {
                context.logger.warn("Unknown resume token error, clearing tokens: ${e.message}")
                true // Clear tokens
            }

            else -> {
                context.logger.warn("General error with change stream configuration: ${e.message}")
                false // Keep tokens
            }
        }
    }

    /**
     * Determines if a cleanup error is critical and should be propagated.
     */
    fun isCriticalCleanupError(e: Exception): Boolean {
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
     * Determines if the event processor should stop based on the exception type.
     */
    fun shouldEventProcessorStop(e: Exception): Boolean {
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
     * Applies retry delay with exponential backoff and jitter.
     * Enhanced with overflow protection and better logging.
     * @param retryCount Current retry attempt number
     * @return true if the retry loop should break (due to cancellation), false to continue
     */
    suspend fun applyRetryDelay(retryCount: Int): Boolean {
        // Prevent overflow in exponentiation by capping the exponent
        val safeRetryCount = min(retryCount, ChangeStreamConfig.MAX_BACKOFF_EXPONENT)

        // compute backoff multiplier as Double to avoid Long×Long overflow
        val multiplier = ChangeStreamConfig.BACKOFF_MULTIPLIER.pow(safeRetryCount)
        val baseDelay = if (context.config.initialRetryDelay.inWholeMilliseconds <= 0L ||
            multiplier > (Long.MAX_VALUE.toDouble() / context.config.initialRetryDelay.inWholeMilliseconds)
        ) {
            context.config.maxRetryDelay.inWholeMilliseconds
        } else {
            min(
                (context.config.initialRetryDelay.inWholeMilliseconds * multiplier).toLong(),
                context.config.maxRetryDelay.inWholeMilliseconds
            )
        }

        // Add jitter to prevent thundering herd
        val jitter = (baseDelay * ChangeStreamConfig.JITTER_FACTOR * kotlin.random.Random.nextDouble()).toLong()
        val delayMs = baseDelay + jitter

        context.logger.info(
            "Retrying change stream in ${delayMs}ms (attempt ${retryCount + 1}/${context.config.maxRetries}, " +
                "consecutive failures: $consecutiveFailures)"
        )

        return try {
            delay(delayMs)
            false // Continue retrying
        } catch (_: CancellationException) {
            context.logger.info("Retry cancelled")
            true // Break the retry loop
        }
    }

    /**
     * Handles an error and determines what action to take.
     * @param e The exception that occurred
     * @param retryCount Current retry attempt number
     * @return RetryDecision indicating what action to take
     */
    suspend fun handleError(e: Exception, retryCount: Int): RetryDecision {
        recordFailure(e)

        context.logger.warn(
            "Change stream error (failure #$consecutiveFailures): ${e.message}"
        )

        // Check for fatal errors first
        if (isFatalError(e)) {
            context.logger.error(
                "Fatal error detected, stopping change stream"
            )
            return RetryDecision.StopWithError(e)
        }

        // Check if we've exceeded retry limit
        if (retryCount >= context.config.maxRetries) {
            context.logger.error(
                "Change stream failed permanently after $retryCount attempts"
            )
            return RetryDecision.Stop
        }

        // Apply retry delay and check for cancellation
        val shouldBreak = applyRetryDelay(retryCount)
        return if (shouldBreak) {
            RetryDecision.Stop
        } else {
            RetryDecision.Continue
        }
    }
}
