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
                cache.readAllFromDatabase().getOrThrow().toList().size.shouldBe(0)

                // Insert a new document into the database (external operation)
                val key = UUID.randomUUID().toString()
                val doc = TestGenericDoc(
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
                cache.readAllFromDatabase().getOrThrow().toList().size.shouldBe(1)

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
            cache.readAllFromDatabase().getOrThrow().toList().size.shouldBe(1)

            // Update the document in the database (external operation)
            testContainer.manualDocumentUpdate(cache, initialDoc, newVersion = 56)

            // Wait for the change stream to process the update (should be fast)
            delay(1_000)

            // Check cache is updated with the new document
            cache.getCacheSize().shouldBe(1)
            cache.readAll().size.shouldBe(1)
            cache.readSizeFromDatabase().getOrThrow().shouldBe(1)
            cache.readAllFromDatabase().getOrThrow().toList().size.shouldBe(1)

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
            cache.readAllFromDatabase().getOrThrow().toList().size.shouldBe(1)

            // Replace the document in the database (external operation)
            val docReplacement = initialDoc.copy(
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
            cache.readAllFromDatabase().getOrThrow().toList().size.shouldBe(1)

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
            cache.readAllFromDatabase().getOrThrow().toList().size.shouldBe(1)

            // Delete the document in the database (external operation)
            testContainer.manualDocumentDelete(cache, initialDoc.key)

            // Wait for the change stream to process the update (should be fast)
            delay(1_000)

            // Check cache is updated with the new document
            cache.getCacheSize().shouldBe(0)
            cache.readAll().size.shouldBe(0)
            cache.readSizeFromDatabase().getOrThrow().shouldBe(0)
            cache.readAllFromDatabase().getOrThrow().toList().size.shouldBe(0)

            // Check cache read
            val readResult = cache.read(key)
            readResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
            // Check database read
            val dbReadResult = cache.readFromDatabase(key)
            dbReadResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
        }
    }
}
