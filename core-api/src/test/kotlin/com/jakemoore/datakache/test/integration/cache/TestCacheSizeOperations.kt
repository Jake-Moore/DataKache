package com.jakemoore.datakache.test.integration.cache

import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.data.MyData
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.toSet

@Suppress("unused")
class TestCacheSizeOperations : AbstractDataKacheTest() {

    init {
        describe("Cache Size Operations") {

            it("should get cache size from empty cache") {
                cache.getCacheSize().shouldBe(0)
            }

            it("should get cache size with one document") {

                // Create one document
                cache.create("cacheSizeKey1") { doc ->
                    doc.copy(name = "Cache Size Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(1)
            }

            it("should get cache size with multiple documents") {

                // Create multiple documents with unique names and balances
                cache.create("cacheSizeKey1") { doc ->
                    doc.copy(name = "Cache Size Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("cacheSizeKey2") { doc ->
                    doc.copy(name = "Cache Size Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("cacheSizeKey3") { doc ->
                    doc.copy(name = "Cache Size Doc 3", balance = 300.0)
                }.getOrThrow()

                cache.create("cacheSizeKey4") { doc ->
                    doc.copy(name = "Cache Size Doc 4", balance = 400.0)
                }.getOrThrow()

                cache.create("cacheSizeKey5") { doc ->
                    doc.copy(name = "Cache Size Doc 5", balance = 500.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(5)
            }

            it("should verify cache size increases after creating documents") {

                // Initial size should be 0
                cache.getCacheSize().shouldBe(0)

                // Create first document
                cache.create("cacheSizeIncreaseKey1") { doc ->
                    doc.copy(name = "Cache Size Increase Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(1)

                // Create second document
                cache.create("cacheSizeIncreaseKey2") { doc ->
                    doc.copy(name = "Cache Size Increase Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(2)

                // Create third document
                cache.create("cacheSizeIncreaseKey3") { doc ->
                    doc.copy(name = "Cache Size Increase Doc 3", balance = 300.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(3)
            }

            it("should verify cache size remains same after updating documents") {

                // Create a document
                cache.create("cacheSizeUpdateKey") { doc ->
                    doc.copy(name = "Cache Size Update Doc", balance = 100.0)
                }.getOrThrow()

                // Size should be 1
                cache.getCacheSize().shouldBe(1)

                // Update the document
                cache.update("cacheSizeUpdateKey") { doc ->
                    doc.copy(name = "Updated Cache Size Doc", balance = 200.0)
                }.getOrThrow()

                // Size should still be 1 (update doesn't change count)
                cache.getCacheSize().shouldBe(1)
            }

            it("should verify cache size decreases after deleting documents") {

                // Create multiple documents
                cache.create("cacheSizeDeleteKey1") { doc ->
                    doc.copy(name = "Cache Size Delete Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("cacheSizeDeleteKey2") { doc ->
                    doc.copy(name = "Cache Size Delete Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("cacheSizeDeleteKey3") { doc ->
                    doc.copy(name = "Cache Size Delete Doc 3", balance = 300.0)
                }.getOrThrow()

                // Initial size should be 3
                cache.getCacheSize().shouldBe(3)

                // Delete first document
                cache.delete("cacheSizeDeleteKey1").getOrThrow()
                cache.getCacheSize().shouldBe(2)

                // Delete second document
                cache.delete("cacheSizeDeleteKey2").getOrThrow()
                cache.getCacheSize().shouldBe(1)

                // Delete third document
                cache.delete("cacheSizeDeleteKey3").getOrThrow()
                cache.getCacheSize().shouldBe(0)
            }

            it("should verify cache size with documents having complex data") {

                // Create documents with complex data
                cache.create("cacheSizeComplexKey1") { doc ->
                    doc.copy(
                        name = "Complex Cache Size Doc 1",
                        balance = 100.0,
                        list = listOf("item1", "item2"),
                        customList = listOf(MyData.createRandom()),
                        customSet = setOf(MyData.createRandom()),
                        customMap = mapOf("key1" to MyData.createRandom())
                    )
                }.getOrThrow()

                cache.create("cacheSizeComplexKey2") { doc ->
                    doc.copy(
                        name = "Complex Cache Size Doc 2",
                        balance = 200.0,
                        list = listOf("item3", "item4", "item5"),
                        customList = listOf(MyData.createRandom()),
                        customSet = setOf(MyData.createRandom()),
                        customMap = mapOf("key2" to MyData.createRandom())
                    )
                }.getOrThrow()

                cache.getCacheSize().shouldBe(2)
            }

            it("should verify cache size with documents having null values") {

                // Create document with null values
                cache.create("cacheSizeNullKey") { doc ->
                    doc.copy(
                        name = null,
                        balance = 0.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }.getOrThrow()

                cache.getCacheSize().shouldBe(1)
            }

            it("should verify cache size with documents having empty string key") {

                // Create document with empty key
                cache.create("") { doc ->
                    doc.copy(name = "Empty Key Cache Size Doc", balance = 100.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(1)
            }

            it("should verify cache size with documents having special characters in key") {
                val specialKey = "cache-size-test-key_with.special@chars#123"

                // Create document with special key
                cache.create(specialKey) { doc ->
                    doc.copy(name = "Special Key Cache Size Doc", balance = 200.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(1)
            }

            it("should verify cache size consistency with readAll") {

                // Create multiple documents
                cache.create("cacheSizeConsistencyKey1") { doc ->
                    doc.copy(name = "Consistency Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("cacheSizeConsistencyKey2") { doc ->
                    doc.copy(name = "Consistency Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("cacheSizeConsistencyKey3") { doc ->
                    doc.copy(name = "Consistency Doc 3", balance = 300.0)
                }.getOrThrow()

                // Size from getCacheSize
                val cacheSize = cache.getCacheSize()

                // Size from readAll
                val allDocs = cache.readAll()
                val allDocsSize = allDocs.size

                // Both should be the same
                cacheSize.shouldBe(allDocsSize)
                cacheSize.shouldBe(3)
            }

            it("should verify cache size consistency with getKeys") {

                // Create multiple documents
                cache.create("cacheSizeKeysConsistencyKey1") { doc ->
                    doc.copy(name = "Keys Consistency Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("cacheSizeKeysConsistencyKey2") { doc ->
                    doc.copy(name = "Keys Consistency Doc 2", balance = 200.0)
                }.getOrThrow()

                // Size from getCacheSize
                val cacheSize = cache.getCacheSize()

                // Size from getKeys
                val keys = cache.getKeys()
                val keysSize = keys.size

                // Both should be the same
                cacheSize.shouldBe(keysSize)
                cacheSize.shouldBe(2)
            }

            it("should verify cache size after multiple operations sequence") {

                // Initial size should be 0
                cache.getCacheSize().shouldBe(0)

                // Create documents
                cache.create("sequenceKey1") { doc ->
                    doc.copy(name = "Sequence Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("sequenceKey2") { doc ->
                    doc.copy(name = "Sequence Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("sequenceKey3") { doc ->
                    doc.copy(name = "Sequence Doc 3", balance = 300.0)
                }.getOrThrow()

                // Size should be 3
                cache.getCacheSize().shouldBe(3)

                // Update one document
                cache.update("sequenceKey2") { doc ->
                    doc.copy(name = "Updated Sequence Doc 2", balance = 250.0)
                }.getOrThrow()

                // Size should still be 3
                cache.getCacheSize().shouldBe(3)

                // Delete one document
                cache.delete("sequenceKey1").getOrThrow()

                // Size should be 2
                cache.getCacheSize().shouldBe(2)

                // Create another document
                cache.create("sequenceKey4") { doc ->
                    doc.copy(name = "Sequence Doc 4", balance = 400.0)
                }.getOrThrow()

                // Size should be 3
                cache.getCacheSize().shouldBe(3)
            }

            it("should verify cache size with readOrCreate operations") {

                // Initial size should be 0
                cache.getCacheSize().shouldBe(0)

                // ReadOrCreate should create new document
                cache.readOrCreate("readOrCreateKey1") { doc ->
                    doc.copy(name = "ReadOrCreate Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(1)

                // ReadOrCreate should read existing document (no size change)
                cache.readOrCreate("readOrCreateKey1") { doc ->
                    doc.copy(name = "ReadOrCreate Doc 1 Updated", balance = 150.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(1)

                // Create another document
                cache.readOrCreate("readOrCreateKey2") { doc ->
                    doc.copy(name = "ReadOrCreate Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(2)
            }

            it("should verify cache size with random key creation") {

                // Initial size should be 0
                cache.getCacheSize().shouldBe(0)

                // Create document with random key
                cache.createRandom { doc ->
                    doc.copy(name = "Random Key Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(1)

                // Create another document with random key
                cache.createRandom { doc ->
                    doc.copy(name = "Random Key Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.getCacheSize().shouldBe(2)
            }

            it("should verify cache size after cache clear operations") {

                // Create multiple documents
                cache.create("clearKey1") { doc ->
                    doc.copy(name = "Clear Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("clearKey2") { doc ->
                    doc.copy(name = "Clear Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("clearKey3") { doc ->
                    doc.copy(name = "Clear Doc 3", balance = 300.0)
                }.getOrThrow()

                // Size should be 3
                cache.getCacheSize().shouldBe(3)

                // Delete all documents
                cache.delete("clearKey1").getOrThrow()
                cache.delete("clearKey2").getOrThrow()
                cache.delete("clearKey3").getOrThrow()

                // Size should be 0
                cache.getCacheSize().shouldBe(0)
            }

            it("should verify cache size after cache clear all") {

                // Create multiple documents
                cache.create("clearKey1") { doc ->
                    doc.copy(name = "Clear Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("clearKey2") { doc ->
                    doc.copy(name = "Clear Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("clearKey3") { doc ->
                    doc.copy(name = "Clear Doc 3", balance = 300.0)
                }.getOrThrow()

                // Size should be 3
                cache.getCacheSize().shouldBe(3)

                // Delete all documents
                val removed = cache.clearDocsFromDatabasePermanently().getOrThrow()
                removed.shouldBe(3)

                // Size should be 0
                cache.getCacheSize().shouldBe(0)
                cache.readAllFromDatabase().getOrThrow().toSet().shouldBe(emptySet())
            }
        }
    }
}
