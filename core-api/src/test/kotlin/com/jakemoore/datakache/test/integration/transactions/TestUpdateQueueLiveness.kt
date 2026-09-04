package com.jakemoore.datakache.test.integration.transactions

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.exception.update.UpdateQueueShutdownException
import com.jakemoore.datakache.api.exception.update.UpdateQueueStalledException
import com.jakemoore.datakache.api.exception.update.UpdateQueueTooDeepException
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiverPartial
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.core.connections.queues.UpdateQueue
import com.jakemoore.datakache.core.connections.queues.UpdateQueueManager
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlin.coroutines.cancellation.CancellationException

private typealias Executor = suspend (
    DocCache<String, TestGenericDoc>,
    TestGenericDoc,
    (TestGenericDoc) -> TestGenericDoc,
    Boolean,
) -> TestGenericDoc

/** Records which liveness fault was reported, so the metric is asserted rather than assumed. */
private class RecordingReceiver : MetricsReceiverPartial() {
    var stalled: Pair<String, Long>? = null
    var tooDeep: Triple<String, Long, Long>? = null
    var updateFailures = 0

    override fun onDatabaseUpdateFail() {
        updateFailures += 1
    }

    override fun onUpdateQueueStalled(cacheName: String, docKeyString: String, queueDepth: Long) {
        stalled = docKeyString to queueDepth
    }

    override fun onUpdateQueueTooDeep(cacheName: String, docKeyString: String, waitedMs: Long, queueDepth: Long) {
        tooDeep = Triple(docKeyString, waitedMs, queueDepth)
    }
}

/**
 * The wait behind an update used to be a budget derived from database ping, multiplied by queue
 * depth. Ping measures a round trip; the cost it stood in for is a transaction retrying against
 * every other writer of the same document, which ping says nothing about. Under contention the
 * estimate was short, so a busy queue failed as though it were broken.
 *
 * The first three cases are the states that budget could not tell apart: busy, wedged, and
 * outpaced. The rest cover shutdown, where the queue abandons an update for reasons of its own and
 * the caller must still be told in a way they can catch.
 */
@Suppress("unused")
class TestUpdateQueueLiveness : AbstractDataKacheTest() {
    init {
        describe("Update Queue Liveness") {

            it("should not fail a queue that is slow but still completing") {
                // The regression this exists for. Thirty items, each a tenth of the window, so the
                // queue takes about three windows to reach the caller and the wait has to survive
                // two of them expiring. The old budget was a per-item estimate multiplied by the
                // depth and derived from ping, so a queue like this failed as though it were
                // broken. Nothing is wrong: it is simply busy.
                val service = DataKache.storageMode.databaseService
                service.stallWindowMsOverride = 1_000L

                try {
                    val doc = cache.create("livenessSlowKey") { it.copy(name = "livenessSlow") }.getOrThrow()
                    val slow: Executor = { _, d, f, _ ->
                        delay(100L)
                        f(d)
                    }

                    repeat(29) {
                        service.updateQueueManagerInternal.enqueueUpdate(cache, doc, { it }, slow, true)
                    }
                    val queued =
                        service.updateQueueManagerInternal.enqueueUpdate(
                            cache,
                            doc,
                            { it.copy(balance = 1.0) },
                            slow,
                            true,
                        )

                    // Waits across several windows, seeing progress each time, and returns normally.
                    service.awaitUpdate(cache, doc.key, queued).balance.shouldBe(1.0)
                } finally {
                    service.stallWindowMsOverride = null
                }
            }

            it("should fail a queue that completes nothing, naming the stall") {
                // A queue whose item never returns completes nothing, which is the one thing no
                // amount of load explains.
                val service = DataKache.storageMode.databaseService
                service.stallWindowMsOverride = 300L
                val receiver = RecordingReceiver()
                DataKacheMetrics.registerReceiverByID("liveness-stalled", receiver)

                val release = CompletableDeferred<TestGenericDoc>()
                try {
                    val doc = cache.create("livenessStalledKey") { it.copy(name = "livenessStalled") }.getOrThrow()
                    val neverReturns: Executor = { _, _, _, _ -> release.await() }

                    val queued =
                        service.updateQueueManagerInternal.enqueueUpdate(cache, doc, { it }, neverReturns, true)

                    val thrown =
                        shouldThrow<UpdateQueueStalledException> {
                            service.awaitUpdate(cache, doc.key, queued)
                        }
                    thrown.stalledForMs.shouldBe(300L)
                    thrown.docNamespace.shouldBe(cache.getKeyNamespace(doc.key))
                    thrown.queueDepth.shouldBe(1L)
                    receiver.stalled?.first.shouldBe(doc.key)
                    receiver.stalled?.second.shouldBe(1L)
                } finally {
                    // Let the queue finish, or shutdown waits its full timeout on every run.
                    release.complete(TestGenericDoc(key = "released", version = 0L))
                    DataKacheMetrics.unregisterReceiverByID("liveness-stalled")
                    service.stallWindowMsOverride = null
                }
            }

            it("should fail a queue that progresses but never reaches the caller, as a different fault") {
                // Distinguished from a stall on purpose. The queue is healthy and the document is
                // being written faster than it can be written, so the remedy differs and so does
                // the exception. The window is well clear of the per-item cost, so only the ceiling
                // can end this wait.
                val service = DataKache.storageMode.databaseService
                service.stallWindowMsOverride = 1_000L
                // Ceiling below the window, so the very first window that expires trips it. Any
                // arrangement where the ceiling falls between two windows depends on the queue's
                // total taking longer than a whole extra window, which is a race, not a test.
                service.queueCeilingMsOverride = 500L
                val receiver = RecordingReceiver()
                DataKacheMetrics.registerReceiverByID("liveness-deep", receiver)

                try {
                    val doc = cache.create("livenessDeepKey") { it.copy(name = "livenessDeep") }.getOrThrow()
                    val slow: Executor = { _, d, f, _ ->
                        delay(100L)
                        f(d)
                    }

                    repeat(15) {
                        service.updateQueueManagerInternal.enqueueUpdate(cache, doc, { it }, slow, true)
                    }
                    val last =
                        service.updateQueueManagerInternal.enqueueUpdate(cache, doc, { it }, slow, true)

                    val thrown =
                        shouldThrow<UpdateQueueTooDeepException> {
                            service.awaitUpdate(cache, doc.key, last)
                        }
                    thrown.docNamespace.shouldBe(cache.getKeyNamespace(doc.key))
                    // Both carried, and distinguishable: waited is a duration, depth is a count, and
                    // swapping them at the throw site would be invisible without this.
                    (thrown.waitedMs >= 500L).shouldBe(true)
                    (thrown.queueDepth > 0L).shouldBe(true)
                    receiver.tooDeep?.first.shouldBe(doc.key)
                    // Same two values as the exception, in the same order, so a swap at either
                    // site shows up here rather than in a dashboard.
                    ((receiver.tooDeep?.second ?: 0L) >= 500L).shouldBe(true)
                    ((receiver.tooDeep?.third ?: 0L) > 0L).shouldBe(true)
                } finally {
                    DataKacheMetrics.unregisterReceiverByID("liveness-deep")
                    service.stallWindowMsOverride = null
                    service.queueCeilingMsOverride = null
                }
            }

            it("should fail, not cancel, an update abandoned by a queue shutdown") {
                // A queue gives in-flight work a grace period and then cancels it. That is the
                // QUEUE's cancellation, not the caller's, and handing the caller a
                // CancellationException would mark their coroutine cancelled rather than failed:
                // they stop silently instead of seeing something they can handle. This is the same
                // footgun the two liveness exceptions were shaped to avoid, reachable through the
                // shutdown path instead.
                val service = DataKache.storageMode.databaseService
                val doc = cache.create("shutdownAbandonKey") { it.copy(name = "shutdownAbandon") }.getOrThrow()

                val release = CompletableDeferred<TestGenericDoc>()
                val neverReturns: Executor = { _, _, _, _ -> release.await() }
                val queued =
                    service.updateQueueManagerInternal.enqueueUpdate(cache, doc, { it }, neverReturns, true)

                // Force the queue past its grace period while the item is still running.
                queued.queue?.shutdown(timeoutMs = 200L)

                val thrown =
                    shouldThrow<UpdateQueueShutdownException> {
                        queued.deferred.await()
                    }
                thrown.docNamespace.shouldBe(cache.getKeyNamespace(doc.key))
                release.complete(TestGenericDoc(key = "released", version = 0L))
            }

            it("should fail, not cancel, updates still waiting when a shutdown is forced") {
                // The in-flight item above is only one of the ways shutdown ends an update. Anything
                // still sitting in the channel is never dequeued at all, and is failed by the drain
                // loop instead, which is a completely separate line of code and was the last place
                // still handing the caller a raw CancellationException.
                val service = DataKache.storageMode.databaseService
                val doc = cache.create("shutdownDrainKey") { it.copy(name = "shutdownDrain") }.getOrThrow()

                val release = CompletableDeferred<TestGenericDoc>()
                val neverReturns: Executor = { _, _, _, _ -> release.await() }

                // Whichever of these the queue dequeues first blocks it forever, so the rest are
                // still in the channel when the grace period expires. That holds at any speed.
                val queued =
                    (0 until 4).map {
                        service.updateQueueManagerInternal.enqueueUpdate(cache, doc, { it }, neverReturns, true)
                    }

                queued.first().queue?.shutdown(timeoutMs = 200L)

                queued.forEach { update ->
                    val thrown = shouldThrow<UpdateQueueShutdownException> { update.deferred.await() }
                    thrown.docNamespace.shouldBe(cache.getKeyNamespace(doc.key))
                }
                release.complete(TestGenericDoc(key = "released", version = 0L))
            }

            it("should never hand a caller a cancellation when the queue is full at shutdown") {
                // Backpressure is the third way a queue refuses an update, and it had the same shape
                // of bug as the other two: one catch-all forwarding whatever it caught, a
                // CancellationException included, straight to the caller. Which branch runs depends
                // on how quickly the channel closes, so this asserts the property they share rather
                // than naming one exception.
                val service = DataKache.storageMode.databaseService
                val doc = cache.create("shutdownFullKey") { it.copy(name = "shutdownFull") }.getOrThrow()

                val release = CompletableDeferred<TestGenericDoc>()
                val neverReturns: Executor = { _, _, _, _ -> release.await() }

                // One in flight and a full channel behind it, so the last of these has nowhere to
                // go and takes the backpressure path.
                val queued =
                    (0..UpdateQueue.MAX_QUEUED_UPDATES + 1).map {
                        service.updateQueueManagerInternal.enqueueUpdate(cache, doc, { it }, neverReturns, true)
                    }

                queued.first().queue?.shutdown(timeoutMs = 200L)

                queued.forEach { update ->
                    val thrown = shouldThrow<Exception> { update.deferred.await() }
                    (thrown is CancellationException).shouldBe(false)
                }
                release.complete(TestGenericDoc(key = "released", version = 0L))
            }

            it("should report a shutdown as a failure, not an update failure, through the public path") {
                // The three queue faults are faults of the queue rather than of the database, so
                // none of them counts towards onDatabaseUpdateFail. Asserted through cache.update
                // because that is the only path a consumer has, and because it is where a
                // CancellationException would do its damage.
                val service = DataKache.storageMode.databaseService
                val receiver = RecordingReceiver()
                DataKacheMetrics.registerReceiverByID("liveness-shutdown-public", receiver)
                try {
                    val doc = cache.create("shutdownPublicKey") { it.copy(name = "shutdownPublic") }.getOrThrow()

                    // Shut the queue down while leaving it registered under this key, so the next
                    // update through the public path finds it and is refused.
                    val warmup =
                        service.updateQueueManagerInternal.enqueueUpdate(
                            cache,
                            doc,
                            { it },
                            { _, d, f, _ -> f(d) },
                            true,
                        )
                    warmup.deferred.await()
                    warmup.queue?.shutdown()

                    val result = cache.update(doc.key) { it.copy(balance = 5.0) }
                    val failure = result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                    failure.exception.exception.shouldBeInstanceOf<UpdateQueueShutdownException>()
                    receiver.updateFailures.shouldBe(0)
                } finally {
                    DataKacheMetrics.unregisterReceiverByID("liveness-shutdown-public")
                }
            }

            it("should hold no queues and refuse new updates once the manager is shut down") {
                // A queue created after shutdown has taken its snapshot appears in no snapshot, is
                // never shut down, and leaves a processing coroutine running an executor against a
                // service that has stopped. Admission is decided under the lock the snapshot is
                // taken with, which is the ordering this pins. Uses its own manager, since shutting
                // down the shared one would take every later test with it.
                val manager = UpdateQueueManager(cache.getLoggerInternal())
                val doc = cache.create("managerShutdownKey") { it.copy(name = "managerShutdown") }.getOrThrow()
                val immediate: Executor = { _, d, f, _ -> f(d) }

                manager.enqueueUpdate(cache, doc, { it }, immediate, true).deferred.await()
                manager.getActiveQueuesCount().shouldBe(1)

                manager.shutdown()
                manager.getActiveQueuesCount().shouldBe(0)

                val refused = manager.enqueueUpdate(cache, doc, { it }, immediate, true)
                shouldThrow<UpdateQueueShutdownException> { refused.deferred.await() }
            }

            it("should still count an ordinary update failure through the public path") {
                // The liveness exceptions skip onDatabaseUpdateFail on purpose, and an earlier
                // version of that skip was written broadly enough to take the pre-existing
                // validation failures with it. Nothing asserted this metric at all, so the
                // undercount would have reached a dashboard before anyone noticed.
                val receiver = RecordingReceiver()
                DataKacheMetrics.registerReceiverByID("liveness-failcount", receiver)
                try {
                    cache.create("publicPathFailKey") { it.copy(name = "publicPathFail") }.getOrThrow()

                    // Returning the same instance is rejected, and is an ordinary failure.
                    cache.update("publicPathFailKey") { it }.isFailure().shouldBe(true)

                    receiver.updateFailures.shouldBe(1)
                } finally {
                    DataKacheMetrics.unregisterReceiverByID("liveness-failcount")
                }
            }
        }
    }
}
