package com.jakemoore.datakache.api.exception.update

import com.jakemoore.datakache.api.exception.DataKacheException

// DocumentUpdateExceptions.kt
open class DocumentUpdateException(message: String) : DataKacheException(message)

/**
 * Thrown when the queue serialising updates for a document stopped making progress.
 *
 * Not a timeout on the caller's own update. The queue completed nothing at all for
 * [stalledForMs], which no amount of load explains, so something is wedged rather than busy.
 *
 * Deliberately not a `CancellationException`. The previous behaviour let a
 * `TimeoutCancellationException` escape a suspend function that had not been cancelled, which
 * cancels the caller's scope instead of surfacing as a failure they can catch.
 *
 * **The outcome of the update is unknown, so do not blindly retry it.** Only the waiting was
 * abandoned. The queue still owns the request and may complete it moments later, so retrying an
 * update that is not idempotent can apply it twice.
 */
class UpdateQueueStalledException(val docNamespace: String, val stalledForMs: Long, val queueDepth: Long) :
    DocumentUpdateException(
        "[$docNamespace] Update queue completed nothing for ${stalledForMs}ms with $queueDepth waiting. " +
            "The queue is stalled rather than slow.",
    )

/**
 * Thrown when the queue for a document kept making progress but the caller's turn never arrived.
 *
 * Distinct from [UpdateQueueStalledException] because the remedy is different: this is a document
 * being written far faster than it can be written, not a fault in the queue.
 *
 * **The outcome of the update is unknown, so do not blindly retry it**, for the same reason as
 * [UpdateQueueStalledException]: the queue still owns the request and may complete it later.
 */
class UpdateQueueTooDeepException(val docNamespace: String, val waitedMs: Long, val queueDepth: Long) :
    DocumentUpdateException(
        "[$docNamespace] Update did not reach the front of the queue within ${waitedMs}ms, with " +
            "$queueDepth still waiting. The queue is progressing; it is receiving work faster than it " +
            "can complete it.",
    )

/**
 * Thrown when a shutting-down queue would not or could not finish an update.
 *
 * Covers every way shutdown ends an update: refused at enqueue, still waiting in the queue when the
 * grace period expired, or in flight when the queue was cancelled.
 *
 * Deliberately not a `CancellationException`. The queue's cancellation is **not the caller's**, and
 * one escaping a suspend call that was never cancelled marks the caller's own coroutine cancelled
 * rather than failed, so the caller silently stops instead of seeing a failure it can handle.
 *
 * **Treat the outcome as unknown.** An update abandoned in flight may have reached the database
 * before the cancellation landed, and this exception does not distinguish that from the cases where
 * nothing was written. At shutdown the remedy is the same either way.
 */
class UpdateQueueShutdownException(val docNamespace: String) :
    DocumentUpdateException(
        "[$docNamespace] Update queue shut down before this update finished. It may or may not " +
            "have been applied.",
    )

/** Thrown when the update function returns the _same_ instance. */
class UpdateFunctionReturnedSameInstanceException(val docNamespace: String) :
    DocumentUpdateException(
        "[$docNamespace] Update function must return a new doc (using data class copy)",
    )

/** Thrown when the key of the updated document doesn’t match the expected key. */
class IllegalDocumentKeyModificationException(
    val docNamespace: String,
    val foundKeyString: String,
    val expectedKeyString: String,
) : DocumentUpdateException(
    "[$docNamespace] Updated doc key mismatch! Found: $foundKeyString, Expected: $expectedKeyString",
)

/** Thrown when the version on the updated document isn’t the expected version. */
class IllegalDocumentVersionModificationException(
    val docNamespace: String,
    val foundVersion: Long,
    val expectedVersion: Long,
) : DocumentUpdateException(
    "[$docNamespace] Updated doc version mismatch! Found: $foundVersion, Expected: $expectedVersion",
)
