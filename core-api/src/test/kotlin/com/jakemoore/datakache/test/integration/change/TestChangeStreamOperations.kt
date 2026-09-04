package com.jakemoore.datakache.test.integration.change

import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

@Suppress("unused")
class TestChangeStreamOperations : AbstractDataKacheTest() {
    init {
        describe("Change Stream Operations") {

            it("should expose change stream buffer depth, and reset the peak when read") {
                // The buffer is what makes a slow consumer visible instead of silent. A full one
                // pauses the stream rather than dropping or reordering anything, so depth
                // approaching capacity is the cache falling behind, and it is worth a gauge.
                eventually(5.seconds) {
                    delay(100)
                    require(cache.areChangeStreamJobsRunning())
                }

                val initial = cache.getChangeStreamQueueStats()
                require(initial != null) { "expected buffer stats while the stream is running" }
                require(initial.capacity > 0) { "expected a bounded buffer, got ${initial.capacity}" }

                repeat(5) { i ->
                    cache
                        .create("queueStatsKey$i") { it.copy(name = "queueStats$i", balance = 860.0 + i) }
                        .getOrThrow()
                }

                // Fixed wait rather than polling, because every read resets the peak and polling
                // would destroy the evidence this asserts on.
                delay(1_000)

                val afterWrites = cache.getChangeStreamQueueStats()
                require(afterWrites != null)
                afterWrites.depth.shouldBe(0)
                require(afterWrites.peakSinceLastRead >= 1) {
                    "expected the buffer to have held at least one event, got ${afterWrites.peakSinceLastRead}"
                }

                // The read above reset it, so a quiet interval reports the buffer as it stands.
                cache
                    .getChangeStreamQueueStats()
                    ?.peakSinceLastRead
                    .shouldBe(0)
            }

            it("should advance the point the stream would resume from as it applies events") {
                // The regression this exists for. The operation-time fallback was set once, at
                // cache start, and never moved. Losing both resume tokens therefore replayed every
                // change since the process booted: an enormous replay on a long-lived cache, or,
                // once the oplog no longer reaches back that far, a resume that fails and silently
                // restarts from the current time with everything in between missed.
                eventually(5.seconds) {
                    delay(100)
                    require(cache.areChangeStreamJobsRunning())
                }

                val atStart = cache.streamResumePositionInternal()
                require(atStart != null) { "expected a resume position captured at cache start" }

                repeat(3) { i ->
                    cache
                        .create("resumeAdvanceKey$i") {
                            it.copy(name = "resumeAdvance$i", balance = 850.0 + i)
                        }.getOrThrow()
                }

                // Only the ordered path moves it, so this also waits for those events to be applied.
                eventually(5.seconds) {
                    delay(100)
                    val now = cache.streamResumePositionInternal()
                    require(now != null && now > atStart) {
                        "resume position did not advance past $atStart (still $now)"
                    }
                }
            }

            it("should replicate external INSERT to local cache") {
                // Ensure that the change streams are running before we proceed
                eventually(5.seconds) {
                    delay(100)
                    require(cache.areChangeStreamJobsRunning())
                }

                // Check cache is empty before operation
                cache.getCacheSize().shouldBe(0)
                cache.readAll().size.shouldBe(0)
                cache.readSizeFromDatabase().getOrThrow().shouldBe(0)
                cache
                    .readAllFromDatabase()
                    .getOrThrow()
                    .toList()
                    .size
                    .shouldBe(0)

                // Insert a new document into the database (external operation)
                val key = UUID.randomUUID().toString()
                val doc =
                    TestGenericDoc(
                        key = key,
                        version = 42,
                        name = "Test External Insert",
                        balance = 1024.0,
                    )
                testContainer.manualDocumentInsert(cache, doc)

                // Wait for the change stream to process the insert (should be fast)
                delay(1_000)

                // Check cache is updated with the new document
                cache.getCacheSize().shouldBe(1)
                cache.readAll().size.shouldBe(1)
                cache.readSizeFromDatabase().getOrThrow().shouldBe(1)
                cache
                    .readAllFromDatabase()
                    .getOrThrow()
                    .toList()
                    .size
                    .shouldBe(1)

                // Check cache read
                val readResult = cache.read(key)
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                val readDoc = readResult.value
                readDoc.key shouldBe key
                readDoc.version shouldBe 42
                readDoc.name shouldBe "Test External Insert"
                readDoc.balance shouldBe 1024.0
            }
        }

        it("should replicate external UPDATE to local cache") {
            // Make a document with the cache (so that it exists in the cache)
            val key = UUID.randomUUID().toString()
            val initialDoc = cache.create(key).getOrThrow()

            // Check cache state
            cache.getCacheSize().shouldBe(1)
            cache.readAll().size.shouldBe(1)
            cache.readSizeFromDatabase().getOrThrow().shouldBe(1)
            cache
                .readAllFromDatabase()
                .getOrThrow()
                .toList()
                .size
                .shouldBe(1)

            // Update the document in the database (external operation)
            testContainer.manualDocumentUpdate(cache, initialDoc, newVersion = 56)

            // Wait for the change stream to process the update (should be fast)
            delay(1_000)

            // Check cache is updated with the new document
            cache.getCacheSize().shouldBe(1)
            cache.readAll().size.shouldBe(1)
            cache.readSizeFromDatabase().getOrThrow().shouldBe(1)
            cache
                .readAllFromDatabase()
                .getOrThrow()
                .toList()
                .size
                .shouldBe(1)

            // Check cache read
            val readResult = cache.read(key)
            readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
            val readDoc = readResult.value
            readDoc.key shouldBe key
            readDoc.version shouldBe 56 // Updated version
            readDoc.name shouldBe initialDoc.name // Name should remain unchanged
            readDoc.balance shouldBe initialDoc.balance // Balance should remain unchanged
        }

        it("should replicate external REPLACE to local cache") {
            // Make a document with the cache (so that it exists in the cache)
            val key = UUID.randomUUID().toString()
            val initialDoc = cache.create(key).getOrThrow()

            // Check cache state
            cache.getCacheSize().shouldBe(1)
            cache.readAll().size.shouldBe(1)
            cache.readSizeFromDatabase().getOrThrow().shouldBe(1)
            cache
                .readAllFromDatabase()
                .getOrThrow()
                .toList()
                .size
                .shouldBe(1)

            // Replace the document in the database (external operation)
            val docReplacement =
                initialDoc.copy(
                    version = 72,
                    name = "Test External Replace",
                    balance = 2048.0,
                )
            testContainer.manualDocumentReplace(cache, docReplacement)

            // Wait for the change stream to process the update (should be fast)
            delay(1_000)

            // Check cache is updated with the new document
            cache.getCacheSize().shouldBe(1)
            cache.readAll().size.shouldBe(1)
            cache.readSizeFromDatabase().getOrThrow().shouldBe(1)
            cache
                .readAllFromDatabase()
                .getOrThrow()
                .toList()
                .size
                .shouldBe(1)

            // Check cache read
            val readResult = cache.read(key)
            readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
            val readDoc = readResult.value
            readDoc.key shouldBe key
            readDoc.version shouldBe docReplacement.version // Updated version
            readDoc.name shouldBe docReplacement.name // Updated name
            readDoc.balance shouldBe docReplacement.balance // Updated balance
        }

        it("should replicate external DELETE to local cache") {
            // Make a document with the cache (so that it exists in the cache)
            val key = UUID.randomUUID().toString()
            val initialDoc = cache.create(key).getOrThrow()

            // Check cache state
            cache.getCacheSize().shouldBe(1)
            cache.readAll().size.shouldBe(1)
            cache.readSizeFromDatabase().getOrThrow().shouldBe(1)
            cache
                .readAllFromDatabase()
                .getOrThrow()
                .toList()
                .size
                .shouldBe(1)

            // Delete the document in the database (external operation)
            testContainer.manualDocumentDelete(cache, initialDoc.key)

            // Wait for the change stream to process the update (should be fast)
            delay(1_000)

            // Check cache is updated with the new document
            cache.getCacheSize().shouldBe(0)
            cache.readAll().size.shouldBe(0)
            cache.readSizeFromDatabase().getOrThrow().shouldBe(0)
            cache
                .readAllFromDatabase()
                .getOrThrow()
                .toList()
                .size
                .shouldBe(0)

            // Check cache read
            val readResult = cache.read(key)
            readResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
            // Check database read
            val dbReadResult = cache.readFromDatabase(key)
            dbReadResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
        }
    }
}
