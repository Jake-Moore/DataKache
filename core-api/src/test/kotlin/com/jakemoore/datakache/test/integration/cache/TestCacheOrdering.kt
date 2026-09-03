package com.jakemoore.datakache.test.integration.cache

import com.jakemoore.datakache.api.ordering.OperationTime
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import io.kotest.matchers.shouldBe

/**
 * The cache has two writers, the local write path and the change stream, and they apply the same
 * mutations at different moments. These cover the ordering that keeps them from undoing each other.
 *
 * A change stream event is applied by calling the same cache methods the event handler calls, with
 * the event's operation time, so these exercise the real path without needing to provoke a race.
 */
@Suppress("unused")
class TestCacheOrdering : AbstractDataKacheTest() {
    /**
     * A synthetic operation time that is unambiguously later than anything the database produced.
     *
     * Cluster time packs seconds into the high word, so real values sit near 7.7e18. These sit above
     * that in unsigned space, which is the order [OperationTime] compares in, so a document created
     * by a real write can still be superseded by one of these.
     */
    private fun laterThanAnyWrite(n: Long) = OperationTime(Long.MIN_VALUE + n)

    init {
        describe("Cache Ordering") {

            it("should not resurrect a deleted document when an older event arrives") {
                // The regression this exists for. A document is created, then deleted, and the
                // INSERT event for the create is delivered afterwards carrying its original,
                // earlier operation time. Before ordering existed this put the document back.
                val doc = cache.create("orderingResurrect").getOrThrow()
                val createdAt = laterThanAnyWrite(10L)
                cache.cacheInternal(doc, createdAt)

                cache.uncacheInternal(doc.key, laterThanAnyWrite(20L))
                cache.read(doc.key).isEmpty().shouldBe(true)

                cache.cacheInternal(doc, createdAt)

                cache.read(doc.key).isEmpty().shouldBe(true)
            }

            it("should not overwrite newer state with an older event") {
                val doc = cache.create("orderingStale") { it.copy(balance = 1.0) }.getOrThrow()
                val newer = doc.copy(balance = 99.0)

                cache.cacheInternal(newer, laterThanAnyWrite(30L))
                cache.cacheInternal(doc.copy(balance = 1.0), laterThanAnyWrite(29L))

                cache
                    .read(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(99.0)
            }

            it("should accept a document created again after a delete") {
                // Cluster time never restarts, so this needs no tombstone bookkeeping: the new
                // state is simply later than the delete.
                val doc = cache.create("orderingRecreate").getOrThrow()
                cache.uncacheInternal(doc.key, laterThanAnyWrite(40L))
                cache.read(doc.key).isEmpty().shouldBe(true)

                cache.cacheInternal(doc, laterThanAnyWrite(41L))

                cache.read(doc.key).isEmpty().shouldBe(false)
            }

            it("should treat a redelivered event as a no-op") {
                // A change stream that reconnects resumes from a token and can deliver an event it
                // has already delivered. Equal times are refused, so this cannot undo later state.
                val doc = cache.create("orderingRedelivery") { it.copy(balance = 5.0) }.getOrThrow()
                cache.cacheInternal(doc.copy(balance = 5.0), laterThanAnyWrite(50L))
                cache.cacheInternal(doc.copy(balance = 7.0), laterThanAnyWrite(51L))

                cache.cacheInternal(doc.copy(balance = 5.0), laterThanAnyWrite(50L))

                cache
                    .read(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(7.0)
            }

            it("should refuse an event with no operation time over state that has one") {
                // An event carrying no cluster time is ordered before everything, so it can never
                // overwrite state whose position is known.
                val doc = cache.create("orderingUnknown") { it.copy(balance = 3.0) }.getOrThrow()
                cache.cacheInternal(doc.copy(balance = 3.0), laterThanAnyWrite(60L))

                cache.cacheInternal(doc.copy(balance = 8.0), OperationTime.UNKNOWN)

                cache
                    .read(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(3.0)
            }
        }
    }
}
