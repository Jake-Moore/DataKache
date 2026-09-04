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
                // A scenario check rather than a regression test, and worth saying so. It walks
                // delete, recreate, then enough unrelated deletes to drive real evictions, and
                // asserts the recreated key still refuses a stale event afterwards.
                //
                // What it deliberately does NOT claim: that it would catch a recreate which forgot
                // to drop the key from the removed-key record. It would not. Such a key would be
                // evicted carrying its OLD position, and applyEviction removes conditionally on
                // exactly that value, so the recreate's newer position survives anyway. The
                // conditional removal is what makes this safe, and it is not something a sequential
                // test can distinguish from the tombstone bookkeeping being correct.
                cache.tombstoneLimit = 2

                val doc =
                    cache
                        .create("tombstoneRaceKey") { it.copy(name = "tombstoneRaceKey", balance = 900.0) }
                        .getOrThrow()
                cache.uncacheInternal(doc.key, laterThanAnyWrite(1L))
                cache.cacheInternal(doc, laterThanAnyWrite(2L))

                // Eviction only happens below the ordering boundary, so move it past everything
                // these deletes will record. Without this the record simply grows and no eviction
                // is exercised at all.
                cache.advanceStreamPositionInternal(laterThanAnyWrite(50L))

                // Push several unrelated deletes through to drive eviction of older entries.
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

            it("should not let the public readFromDatabase refresh a key that already has a position") {
                // The same guard as the two cacheContentOnlyInternal cases below, exercised through
                // the public method whose KDoc makes the promise, because the behaviour a consumer
                // sees is the one worth pinning. A read carries no position of its own, so it must
                // defer to a key that has one, and it still hands the caller what it read.
                val doc =
                    cache
                        .create("readFromDbGuardKey") { it.copy(name = "readFromDbGuard", balance = 10.0) }
                        .getOrThrow()

                // Move the cache ahead of the database without touching the database.
                cache.cacheInternal(doc.copy(balance = 42.0), laterThanAnyWrite(2000L))

                // The caller receives what the database holds.
                cache
                    .readFromDatabase(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(10.0)

                // The cache keeps the newer state the read had no position to outrank.
                cache
                    .read(doc.key)
                    .getOrThrow()
                    .balance
                    .shouldBe(42.0)
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

            it("should hold a tombstone the change stream has not applied past") {
                // The record is bounded so it cannot grow forever, but forgetting a key's position
                // makes the next event for it apply unconditionally, so an entry may only be
                // dropped once no event older than it can still be in flight. That boundary is how
                // far the change stream has applied IN ORDER, which matters because the stream
                // applies an event out of band when its buffer saturates, ahead of everything still
                // queued. Evicting on count alone would drop exactly the entry protecting against
                // the older queued event, and nothing later would repair it.
                cache.tombstoneLimit = 1

                val held =
                    cache
                        .create("boundaryHeldKey") { it.copy(name = "boundaryHeld", balance = 700.0) }
                        .getOrThrow()
                cache.uncacheInternal(held.key, laterThanAnyWrite(1000L))

                // Over the limit, so a count-based record would evict the first key here. The
                // stream has applied nowhere near these synthetic positions, so this one may not.
                val other =
                    cache
                        .create("boundaryOtherKey") { it.copy(name = "boundaryOther", balance = 701.0) }
                        .getOrThrow()
                cache.uncacheInternal(other.key, laterThanAnyWrite(1001L))

                // The position survived, so the stale event is still refused.
                cache.cacheInternal(held, laterThanAnyWrite(999L))

                cache.read(held.key).isEmpty().shouldBe(true)
            }

            it("should evict a tombstone once the change stream has applied past it") {
                // The other half, and why the record stays bounded rather than leaking every key
                // ever deleted. Once the stream has applied in order past an entry's position, no
                // event older than it can still be queued, so the entry is safe to forget.
                cache.tombstoneLimit = 1

                val evictable =
                    cache
                        .create("boundaryEvictKey") { it.copy(name = "boundaryEvict", balance = 800.0) }
                        .getOrThrow()
                cache.uncacheInternal(evictable.key, laterThanAnyWrite(2000L))

                // The ordered path reports that it has applied past that position.
                cache.advanceStreamPositionInternal(laterThanAnyWrite(2500L))

                val other =
                    cache
                        .create("boundaryEvictOtherKey") { it.copy(name = "boundaryEvictOther", balance = 801.0) }
                        .getOrThrow()
                cache.uncacheInternal(other.key, laterThanAnyWrite(2600L))

                // The position was forgotten, which is only observable as an older event applying.
                // Safe in production precisely because the boundary says no such event can exist.
                cache.cacheInternal(evictable, laterThanAnyWrite(1999L))

                cache.read(evictable.key).isEmpty().shouldBe(false)
            }

            it("should drop a tombstone unsafely once the record outgrows its ceiling") {
                // Memory wins over ordering in the last resort. A stream that is stopped or far
                // behind never moves the boundary, so the record would otherwise hold every removed
                // key for the life of the process. Past a multiple of the limit the oldest entry is
                // dropped anyway and the breach is logged, so a degraded cache says so rather than
                // being killed for exhausting memory.
                cache.tombstoneLimit = 1

                val first =
                    cache
                        .create("ceilingFirstKey") { it.copy(name = "ceilingFirst", balance = 900.0) }
                        .getOrThrow()
                cache.uncacheInternal(first.key, laterThanAnyWrite(3000L))

                // The boundary never moves, so each of these is held rather than evicted, until the
                // record passes the ceiling and the oldest goes regardless.
                repeat(12) { i ->
                    val other =
                        cache
                            .create("ceilingOtherKey$i") {
                                it.copy(name = "ceilingOther$i", balance = 910.0 + i)
                            }.getOrThrow()
                    cache.uncacheInternal(other.key, laterThanAnyWrite(3001L + i))
                }

                cache.cacheInternal(first, laterThanAnyWrite(2999L))

                cache.read(first.key).isEmpty().shouldBe(false)
            }

            it("should not advance the ordering boundary for an out-of-band event") {
                // Delivered through the real handler, because whether an event advances the
                // boundary is decided there and nowhere else. An out-of-band event is applied when
                // the stream's buffer saturates, ahead of everything still queued, so it proves
                // nothing about what has been applied and must not move the boundary. If it did,
                // the entry protecting against those still-queued older events would be evicted.
                cache.tombstoneLimit = 1

                val held =
                    cache
                        .create("outOfBandHeldKey") { it.copy(name = "outOfBandHeld", balance = 750.0) }
                        .getOrThrow()
                cache.uncacheInternal(held.key, laterThanAnyWrite(4000L))

                val unrelated =
                    cache
                        .create("outOfBandOtherKey") { it.copy(name = "outOfBandOther", balance = 751.0) }
                        .getOrThrow()
                cache.changeEventHandlerInternal().onDocumentDeleted(
                    unrelated.key,
                    laterThanAnyWrite(4500L),
                    outOfBand = true,
                )

                val third =
                    cache
                        .create("outOfBandThirdKey") { it.copy(name = "outOfBandThird", balance = 752.0) }
                        .getOrThrow()
                cache.uncacheInternal(third.key, laterThanAnyWrite(4600L))

                // The boundary never moved, so the held entry survived and the stale event is refused.
                cache.cacheInternal(held, laterThanAnyWrite(3999L))

                cache.read(held.key).isEmpty().shouldBe(true)
            }

            it("should advance the ordering boundary for an ordered event") {
                // The other half of the same gate, identical but for outOfBand. An ordered event
                // does mean everything up to it has been applied, so the entry below it is safe to
                // forget, which is observable as the stale event no longer being refused.
                cache.tombstoneLimit = 1

                val evictable =
                    cache
                        .create("orderedHeldKey") { it.copy(name = "orderedHeld", balance = 760.0) }
                        .getOrThrow()
                cache.uncacheInternal(evictable.key, laterThanAnyWrite(4000L))

                val unrelated =
                    cache
                        .create("orderedOtherKey") { it.copy(name = "orderedOther", balance = 761.0) }
                        .getOrThrow()
                cache.changeEventHandlerInternal().onDocumentDeleted(
                    unrelated.key,
                    laterThanAnyWrite(4500L),
                    outOfBand = false,
                )

                val third =
                    cache
                        .create("orderedThirdKey") { it.copy(name = "orderedThird", balance = 762.0) }
                        .getOrThrow()
                cache.uncacheInternal(third.key, laterThanAnyWrite(4600L))

                cache.cacheInternal(evictable, laterThanAnyWrite(3999L))

                cache.read(evictable.key).isEmpty().shouldBe(false)
            }

            it("should forget the ordering boundary when the change stream reconnects") {
                // The regression this exists for. A reconnecting stream can resume from a point
                // EARLIER than it had already reached, because the resume token fallback ends at
                // the operation time captured when the cache started, and replays history from
                // there. Events older than the boundary the previous connection advanced are then
                // delivered after all, so the boundary means nothing across a reconnection and
                // eviction must stop until ordered delivery re-establishes one.
                cache.tombstoneLimit = 1

                val held =
                    cache
                        .create("reconnectHeldKey") { it.copy(name = "reconnectHeld", balance = 770.0) }
                        .getOrThrow()
                cache.uncacheInternal(held.key, laterThanAnyWrite(5000L))

                // A boundary that would permit forgetting it, and then a reconnection.
                cache.advanceStreamPositionInternal(laterThanAnyWrite(5500L))
                cache.changeEventHandlerInternal().onConnected(mayHaveRepositioned = true)

                val other =
                    cache
                        .create("reconnectOtherKey") { it.copy(name = "reconnectOther", balance = 771.0) }
                        .getOrThrow()
                cache.uncacheInternal(other.key, laterThanAnyWrite(5600L))

                cache.cacheInternal(held, laterThanAnyWrite(4999L))

                cache.read(held.key).isEmpty().shouldBe(true)
            }

            it("should keep the ordering boundary across a reconnection that did not reposition") {
                // The counterpart, and the reason the boundary is usable at all. A reconnection that
                // resumed from a resume token starts immediately after the last event applied, so
                // nothing older arrives and the boundary still holds. Discarding it on every
                // reconnection would be safe and would also make it worthless: reconnections are
                // ordinary, and entries would then only ever leave the record by the ceiling, which
                // is the unsafe path that exists as a last resort.
                cache.tombstoneLimit = 1

                val evictable =
                    cache
                        .create("keptBoundaryKey") { it.copy(name = "keptBoundary", balance = 795.0) }
                        .getOrThrow()
                cache.uncacheInternal(evictable.key, laterThanAnyWrite(8000L))

                cache.advanceStreamPositionInternal(laterThanAnyWrite(8500L))
                cache.changeEventHandlerInternal().onConnected(mayHaveRepositioned = false)

                val other =
                    cache
                        .create("keptBoundaryOtherKey") { it.copy(name = "keptBoundaryOther", balance = 796.0) }
                        .getOrThrow()
                cache.uncacheInternal(other.key, laterThanAnyWrite(8600L))

                // Forgotten, because the boundary survived a reconnection that could not go back.
                cache.cacheInternal(evictable, laterThanAnyWrite(7999L))

                cache.read(evictable.key).isEmpty().shouldBe(false)
            }

            it("should not evict an entry from before a reconnection against a boundary from after it") {
                // The residual the reset alone does not close, and the reason entries carry the
                // connection they were minted on. Clearing the boundary at a reconnection is not
                // enough by itself: the new connection re-establishes one within moments, and an
                // entry minted BEFORE the reconnection is still sitting in the record. Its position
                // came from a stream position that the new connection may not have reached yet,
                // because a replay starts earlier, so comparing the two is meaningless and the
                // entry must simply be ineligible until the ceiling forces it.
                cache.tombstoneLimit = 1

                val held =
                    cache
                        .create("epochHeldKey") { it.copy(name = "epochHeld", balance = 790.0) }
                        .getOrThrow()
                cache.uncacheInternal(held.key, laterThanAnyWrite(7000L))

                cache.changeEventHandlerInternal().onConnected(mayHaveRepositioned = true)

                // The new connection establishes a boundary well past the old entry's position.
                // Comparing positions alone would forget it; comparing connections does not.
                cache.advanceStreamPositionInternal(laterThanAnyWrite(7500L))

                val other =
                    cache
                        .create("epochOtherKey") { it.copy(name = "epochOther", balance = 791.0) }
                        .getOrThrow()
                cache.uncacheInternal(other.key, laterThanAnyWrite(7600L))

                cache.cacheInternal(held, laterThanAnyWrite(6999L))

                cache.read(held.key).isEmpty().shouldBe(true)
            }

            it("should not move the ordering boundary backwards within one connection") {
                // A resumed stream can redeliver an event it has already applied, so the boundary
                // must not follow it backwards while the connection is the same one. Only a
                // reconnection, which can genuinely reposition earlier, resets it.
                cache.tombstoneLimit = 1

                val evictable =
                    cache
                        .create("monotonicKey") { it.copy(name = "monotonic", balance = 780.0) }
                        .getOrThrow()
                cache.uncacheInternal(evictable.key, laterThanAnyWrite(6300L))

                cache.advanceStreamPositionInternal(laterThanAnyWrite(6500L))
                cache.advanceStreamPositionInternal(laterThanAnyWrite(6100L))

                val other =
                    cache
                        .create("monotonicOtherKey") { it.copy(name = "monotonicOther", balance = 781.0) }
                        .getOrThrow()
                cache.uncacheInternal(other.key, laterThanAnyWrite(6600L))

                // Evicted, which only holds if the boundary stayed at 6500 rather than dropping to
                // 6100, since the entry sits between the two.
                cache.cacheInternal(evictable, laterThanAnyWrite(6299L))

                cache.read(evictable.key).isEmpty().shouldBe(false)
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
