package com.jakemoore.datakache.api.ordering

import org.jetbrains.annotations.ApiStatus
import java.lang.Long.compareUnsigned

/**
 * A point in the database's own ordering of operations.
 *
 * The cache has two writers, the local write path and the change stream, and they apply the same
 * mutations at different moments. Ordering them requires one clock both can quote. For MongoDB that
 * is cluster time: a change event carries it, and a session reports it for the write it performed.
 *
 * [raw] is opaque and only meaningful for comparison against another value from the same deployment.
 */
@ApiStatus.Internal
@JvmInline
value class OperationTime(val raw: Long) : Comparable<OperationTime> {
    // Cluster time packs seconds into the high word, so it is ordered as unsigned.
    override fun compareTo(other: OperationTime): Int = compareUnsigned(raw, other.raw)

    override fun toString(): String = "OperationTime(${raw ushr Int.SIZE_BITS}.${raw.toInt()})"

    companion object {
        /**
         * Ordered before every real operation time. Used where state must be cached but its
         * position in the ordering is unknown, so that any real event supersedes it.
         */
        @ApiStatus.Internal
        val UNKNOWN: OperationTime = OperationTime(0L)
    }
}
