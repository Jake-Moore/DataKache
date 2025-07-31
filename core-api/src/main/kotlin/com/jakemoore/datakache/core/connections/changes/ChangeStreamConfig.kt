package com.jakemoore.datakache.core.connections.changes

import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Configuration for the change stream connection.
 * This configuration is designed to be database-agnostic with sensible defaults.
 * Use companion methods to create instances.
 */
@ConsistentCopyVisibility
data class ChangeStreamConfig private constructor(
    // Retry Configuration
    /** Initial delay before the first retry attempt. Must be positive. */
    val initialRetryDelay: Duration,

    /** Maximum delay between retry attempts. Must be >= initialRetryDelay. */
    val maxRetryDelay: Duration,

    /** Maximum number of retry attempts. Use Int.MAX_VALUE for unlimited retries. */
    val maxRetries: Int,

    // Event Processing Configuration
    /** Timeout for processing individual change events. Must be positive. */
    val eventProcessingTimeout: Duration,

    /** Maximum events to buffer before applying backpressure. Must be positive. */
    val maxBufferedEvents: Int
) {
    init {
        validateConfiguration()
    }

    /**
     * Validates the configuration parameters for correctness.
     * @throws IllegalArgumentException if any parameter is invalid
     */
    private fun validateConfiguration() {
        require(initialRetryDelay.isPositive()) {
            "Initial retry delay must be positive: $initialRetryDelay"
        }

        require(maxRetryDelay >= initialRetryDelay) {
            "Max retry delay ($maxRetryDelay) must be >= initial retry delay ($initialRetryDelay)"
        }

        require(maxRetries > 0) {
            "Max retries must be positive: $maxRetries"
        }

        require(eventProcessingTimeout.isPositive()) {
            "Event processing timeout must be positive: $eventProcessingTimeout"
        }

        require(maxBufferedEvents > 0) {
            "Max buffered events must be positive: $maxBufferedEvents"
        }
    }

    companion object {
        /**
         * Creates a MongoDB-optimized configuration with recommended settings.
         * This is the primary configuration method for MongoDB databases.
         *
         * @param production Whether this is for a production environment (affects retry settings).
         *
         * @return ChangeStreamConfig optimized for MongoDB
         */
        fun forMongoDB(production: Boolean = true): ChangeStreamConfig {
            return if (production) {
                ChangeStreamConfig(
                    initialRetryDelay = 2.seconds,
                    maxRetryDelay = 60.seconds,
                    maxRetries = Int.MAX_VALUE,
                    eventProcessingTimeout = 30.seconds,
                    maxBufferedEvents = 1000
                )
            } else {
                ChangeStreamConfig(
                    initialRetryDelay = 500.milliseconds,
                    maxRetryDelay = 5.seconds,
                    maxRetries = 20,
                    eventProcessingTimeout = 10.seconds,
                    maxBufferedEvents = 100
                )
            }
        }

        /** Exponential backoff multiplier - hardcoded to optimal value */
        const val BACKOFF_MULTIPLIER = 1.5

        /** Jitter factor to prevent thundering herd - hardcoded to small optimal value */
        const val JITTER_FACTOR = 0.1

        /** Maximum safe exponent for backoff calculation to prevent overflow.
         * Used by retry logic in ChangeStreamErrorHandler. */
        const val MAX_BACKOFF_EXPONENT = 10
    }
}
