package com.jakemoore.datakache.test.integration.cache

import com.jakemoore.datakache.api.ordering.OperationTime
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
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
                // isReplayedEvent defaults false on both calls below, so optimisticCaching cannot
                // engage regardless of version; this exercises the ordering refusal in isolation.
                val doc = cache.create("orderingStale") { it.copy(balance = 1.0) }.getOrThrow()
                val newer = doc.copy(balance = 99.0)
                val staleAgain = doc.copy(balance = 1.0)

                cache.cacheInternal(newer, laterThanAnyWrite(30L))
                cache.cacheInternal(staleAgain, laterThanAnyWrite(29L))

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
                // A change stream that reconnects resumes from a token and can redeliver an event
                // it has already delivered, carrying the SAME operation time rather than an older
                // one. So the equality half of "not strictly newer" is the one this needs, and it
                // is the same rule a local write and its own change stream echo rely on: those two
                // are one write seen twice and quote one identical time, so whichever arrives
                // second must be refused rather than applied again.
                val doc = cache.create("orderingRedelivery") { it.copy(balance = 5.0) }.getOrThrow()
                cache.cacheInternal(doc.copy(balance = 5.0), laterThanAnyWrite(50L))

                // Equal time, conflicting content: refused, so the content at that position stands.
                cache.cacheInternal(doc.copy(balance = -1.0), laterThanAnyWrite(50L))
                cache
                    .read(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(5.0)

                // A genuinely newer event is still accepted, so the refusal above is the equality
                // rule rather than the position having become stuck.
                cache.cacheInternal(doc.copy(balance = 7.0), laterThanAnyWrite(51L))
                cache
                    .read(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(7.0)

                // And redelivery of the earlier event after that is refused too, on the older half.
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

            it("should skip an optimistic re-cache of a REPLAYED event but still advance the position") {
                // optimisticCaching (default on) skips the map write when the version already
                // matches, on the theory that an equal version means equal data, and it applies
                // only to isReplayedEvent = true, which is how a change stream event is applied and
                // is the only place "same version" is trustworthy evidence of "same content": the
                // version came from a write this cache already saw. A local write must never take
                // this shortcut on its own content, which is what
                // "should not lose an authoritative local write to optimisticCaching" below covers.
                //
                // It must not skip advancing the recorded position, or a later stale event that DID
                // change something would be wrongly accepted because the position never moved past.
                val doc = cache.create("optimisticAdvances") { it.copy(balance = 1.0) }.getOrThrow()

                // Same version, different content: the write is skipped by the optimization.
                cache.cacheInternal(doc.copy(balance = 999.0), laterThanAnyWrite(70L), isReplayedEvent = true)
                cache
                    .read(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(1.0)

                // The position advanced anyway: an event older than the skip is now refused too.
                cache.cacheInternal(doc.copy(balance = -1.0), laterThanAnyWrite(69L), isReplayedEvent = true)
                cache
                    .read(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(1.0)
            }

            it("should not lose an authoritative local write to optimisticCaching, even at an unchanged version") {
                // The regression this exists for. PlayerDocCache.delete() resets a document via a
                // database replace that intentionally does not bump version (it is a reset, not an
                // increment), and the previously cached version was the same. Without
                // isReplayedEvent gating this correctly, that authoritative write was silently
                // skipped, leaving the cache showing pre-reset content indefinitely: the position
                // still advanced, so no later event -- carrying that exact same operation time --
                // could ever arrive to apply what the local write itself had skipped.
                val doc = cache.create("localWriteSameVersion") { it.copy(balance = 1.0) }.getOrThrow()
                val reset = doc.copy(balance = 0.0, version = doc.version)

                cache.cacheInternal(reset, laterThanAnyWrite(80L))

                cache
                    .read(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(0.0)
            }

            it("should not lose a live key's position to tombstone eviction after a delete then a recreate") {
                // This is sequential, so it cannot exercise the CONCURRENT race the fix closes --
                // that atomicity comes from tombstone membership being decided inside the same
                // appliedAt.compute call as the ordering decision, which ConcurrentHashMap
                // guarantees is serialized per key, and is not something a sequential test can
                // disprove. What this covers, and what a naive "clear on delete, forget on cache"
                // implementation would still get wrong even without any race: a recreate must
                // actually remove the key from the tombstone list, or unrelated deletes pushed
                // through afterward can evict it as if it were still tombstoned, evicting a LIVE
                // key's position and reopening the exact refusal a stale event depends on.
                cache.tombstoneLimit = 2

                val doc =
                    cache
                        .create("tombstoneRaceKey") { it.copy(name = "tombstoneRaceKey", balance = 900.0) }
                        .getOrThrow()
                cache.uncacheInternal(doc.key, laterThanAnyWrite(1L))
                cache.cacheInternal(doc, laterThanAnyWrite(2L))

                // Push several unrelated deletes through to force eviction of older tombstones.
                // Distinct name and balance: TestGenericDocCache enforces a unique index on both,
                // and a MongoDB unique index treats every null as a collision unless sparse.
                repeat(5) { i ->
                    val other =
                        cache
                            .create("tombstoneRaceOther$i") {
                                it.copy(name = "tombstoneRaceOther$i", balance = 910.0 + i)
                            }.getOrThrow()
                    cache.uncacheInternal(other.key, laterThanAnyWrite(10L + i))
                }

                // Still live, and still protected: a stale event between the delete and the
                // recreate must be refused, which only holds if the key's position survived.
                cache.read(doc.key).isEmpty().shouldBe(false)
                cache.cacheInternal(doc.copy(balance = -1.0), laterThanAnyWrite(1L))
                cache
                    .read(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(doc.balance)
            }

            it("should clear ordering state along with cached documents on a full clear") {
                // clearDocsFromDatabasePermanently and the other lifecycle clears (startup
                // failure, shutdown, collection drop/rename) remove keys outside the ordered
                // per-key path. If they cleared cacheMap without also clearing appliedAt, every
                // key's position would leak for the life of the process, unrecoverable on a
                // collection drop since a real drop emits no per-document delete events to ever
                // reroute those keys through the bookkeeping that would otherwise free them.
                val doc = cache.create("clearOrderingKey").getOrThrow()
                cache.cacheInternal(doc, laterThanAnyWrite(500L))

                cache.clearDocsFromDatabasePermanently().getOrThrow()

                // If appliedAt still held the old high-water mark, this lower synthetic time
                // would be refused and the document would never reappear in the cache.
                cache.cacheInternal(doc, laterThanAnyWrite(1L))

                cache.read(doc.key).isEmpty().shouldBe(false)
            }

            it("should refuse cacheContentOnlyInternal for a key that already has a position") {
                // The regression this guards: a database read used to write straight into
                // cacheMap with no lock at all, so a slow read could land after a newer write and
                // silently overwrite it, and because appliedAt still held the newer position, the
                // event that would have repaired the cache was then refused as stale too. A read
                // must defer entirely to whatever already has a position, not merely to whatever
                // is currently in cacheMap, which is why this call takes no operation time of
                // its own: it cannot outrank a write it did not observe.
                val doc = cache.create("readGuardKey") { it.copy(balance = 1.0) }.getOrThrow()
                cache.cacheInternal(doc.copy(balance = 2.0), laterThanAnyWrite(90L))

                cache.cacheContentOnlyInternal(doc.copy(balance = 999.0))

                cache
                    .read(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(2.0)
            }

            it("should uncache a key the database reports it did not delete, while the cache holds it") {
                // A read populates a key with no position, so a key can be cached having never gone
                // through the ordered write path. If it is then deleted remotely, or was never in
                // the database at all, the local delete matches no rows. Converging the cache is
                // still this call's job: leaving the document readable after delete() returned
                // would be the same surprise the ordering work exists to remove, arrived at from
                // the other side.
                val stray =
                    TestGenericDoc(
                        key = "strayCachedKey",
                        version = 0L,
                        name = "strayCachedKey",
                        balance = 55.0,
                    )
                cache.cacheContentOnlyInternal(stray)
                cache.read(stray.key).isEmpty().shouldBe(false)

                cache.delete(stray.key).getOrThrow()

                cache.read(stray.key).isEmpty().shouldBe(true)
            }

            it("should stop refusing stale events once a key's tombstone has been evicted") {
                // Pins the documented bound rather than asserting it away. The tombstone record is
                // finite, and a key evicted from it has no position left, so the next event for it
                // is applied unconditionally however old it is. Ordinary delivery is in commit
                // order so nothing older is still in flight; the change stream's out-of-band
                // fallback is the exception, and closing that needs eviction to follow how far the
                // stream has applied rather than a count. This test exists to fail loudly if
                // eviction semantics change, and to keep the limit visible in the suite.
                cache.tombstoneLimit = 1

                val evicted =
                    cache
                        .create("evictedTombstoneKey") { it.copy(name = "evictedTombstone", balance = 700.0) }
                        .getOrThrow()
                cache.uncacheInternal(evicted.key, laterThanAnyWrite(1000L))

                // A second delete pushes the first key's tombstone, and its position, out.
                val other =
                    cache
                        .create("evictingOtherKey") { it.copy(name = "evictingOther", balance = 701.0) }
                        .getOrThrow()
                cache.uncacheInternal(other.key, laterThanAnyWrite(1001L))

                // Older than the delete that removed it, and applied anyway: no position remains.
                cache.cacheInternal(evicted, laterThanAnyWrite(999L))

                cache.read(evicted.key).isEmpty().shouldBe(false)
            }

            it("should populate cacheContentOnlyInternal for a key with no position yet") {
                // The other half: a key nothing has ever cached or deleted has no position to
                // defer to, so a read is the only source of truth and must populate it. Built
                // directly rather than through create(), which would give the key a position,
                // and deleting it again would leave a tombstoned one.
                val fresh =
                    TestGenericDoc(
                        key = "readGuardNeverCachedKey",
                        version = 0L,
                        name = "readGuardNeverCachedKey",
                        balance = 4.0,
                    )

                cache.cacheContentOnlyInternal(fresh)

                cache
                    .read(fresh.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(4.0)
            }
        }
    }
}
