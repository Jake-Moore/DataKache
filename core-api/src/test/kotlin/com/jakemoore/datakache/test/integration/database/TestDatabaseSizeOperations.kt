package com.jakemoore.datakache.test.integration.database

import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.data.MyData
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.flow.toList

@Suppress("unused")
class TestDatabaseSizeOperations : AbstractDataKacheTest() {

    init {
        describe("Database Size Operations") {

            it("should read size from empty database") {
                val sizeResult = cache.readSizeFromDatabase()

                sizeResult.shouldBeInstanceOf<Success<Long>>()
                sizeResult.getOrThrow().shouldBe(0L)
            }

            it("should read size from database with one document") {

                // Create one document
                cache.create("sizeTestKey1") { doc ->
                    doc.copy(name = "Size Test Doc 1", balance = 100.0)
                }.getOrThrow()

                val sizeResult = cache.readSizeFromDatabase()
                sizeResult.shouldBeInstanceOf<Success<Long>>()
                sizeResult.getOrThrow().shouldBe(1L)
            }

            it("should read size from database with multiple documents") {

                // Create multiple documents with unique names and balances
                cache.create("sizeTestKey1") { doc ->
                    doc.copy(name = "Size Test Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("sizeTestKey2") { doc ->
                    doc.copy(name = "Size Test Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("sizeTestKey3") { doc ->
                    doc.copy(name = "Size Test Doc 3", balance = 300.0)
                }.getOrThrow()

                cache.create("sizeTestKey4") { doc ->
                    doc.copy(name = "Size Test Doc 4", balance = 400.0)
                }.getOrThrow()

                cache.create("sizeTestKey5") { doc ->
                    doc.copy(name = "Size Test Doc 5", balance = 500.0)
                }.getOrThrow()

                val sizeResult = cache.readSizeFromDatabase()
                sizeResult.shouldBeInstanceOf<Success<Long>>()
                sizeResult.getOrThrow().shouldBe(5L)
            }

            it("should verify size increases after creating documents") {

                // Initial size should be 0
                var sizeResult = cache.readSizeFromDatabase()
                sizeResult.shouldBeInstanceOf<Success<Long>>()
                sizeResult.getOrThrow().shouldBe(0L)

                // Create first document
                cache.create("sizeIncreaseKey1") { doc ->
                    doc.copy(name = "Size Increase Doc 1", balance = 100.0)
                }.getOrThrow()

                sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(1L)

                // Create second document
                cache.create("sizeIncreaseKey2") { doc ->
                    doc.copy(name = "Size Increase Doc 2", balance = 200.0)
                }.getOrThrow()

                sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(2L)

                // Create third document
                cache.create("sizeIncreaseKey3") { doc ->
                    doc.copy(name = "Size Increase Doc 3", balance = 300.0)
                }.getOrThrow()

                sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(3L)
            }

            it("should verify size remains same after updating documents") {

                // Create a document
                cache.create("sizeUpdateKey") { doc ->
                    doc.copy(name = "Size Update Doc", balance = 100.0)
                }.getOrThrow()

                // Size should be 1
                var sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(1L)

                // Update the document
                cache.update("sizeUpdateKey") { doc ->
                    doc.copy(name = "Updated Size Doc", balance = 200.0)
                }.getOrThrow()

                // Size should still be 1 (update doesn't change count)
                sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(1L)
            }

            it("should verify size decreases after deleting documents") {

                // Create multiple documents
                cache.create("sizeDeleteKey1") { doc ->
                    doc.copy(name = "Size Delete Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("sizeDeleteKey2") { doc ->
                    doc.copy(name = "Size Delete Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("sizeDeleteKey3") { doc ->
                    doc.copy(name = "Size Delete Doc 3", balance = 300.0)
                }.getOrThrow()

                // Initial size should be 3
                var sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(3L)

                // Delete first document
                cache.delete("sizeDeleteKey1").getOrThrow()
                sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(2L)

                // Delete second document
                cache.delete("sizeDeleteKey2").getOrThrow()
                sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(1L)

                // Delete third document
                cache.delete("sizeDeleteKey3").getOrThrow()
                sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(0L)
            }

            it("should verify size with documents having complex data") {

                // Create documents with complex data
                cache.create("sizeComplexKey1") { doc ->
                    doc.copy(
                        name = "Complex Size Doc 1",
                        balance = 100.0,
                        list = listOf("item1", "item2"),
                        customList = listOf(MyData.createRandom()),
                        customSet = setOf(MyData.createRandom()),
                        customMap = mapOf("key1" to MyData.createRandom())
                    )
                }.getOrThrow()

                cache.create("sizeComplexKey2") { doc ->
                    doc.copy(
                        name = "Complex Size Doc 2",
                        balance = 200.0,
                        list = listOf("item3", "item4", "item5"),
                        customList = listOf(MyData.createRandom()),
                        customSet = setOf(MyData.createRandom()),
                        customMap = mapOf("key2" to MyData.createRandom())
                    )
                }.getOrThrow()

                val sizeResult = cache.readSizeFromDatabase()
                sizeResult.shouldBeInstanceOf<Success<Long>>()
                sizeResult.getOrThrow().shouldBe(2L)
            }

            it("should verify size with documents having null values") {

                // Create document with null values
                cache.create("sizeNullKey") { doc ->
                    doc.copy(
                        name = null,
                        balance = 0.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }.getOrThrow()

                val sizeResult = cache.readSizeFromDatabase()
                sizeResult.shouldBeInstanceOf<Success<Long>>()
                sizeResult.getOrThrow().shouldBe(1L)
            }

            it("should verify size with documents having empty string key") {

                // Create document with empty key
                cache.create("") { doc ->
                    doc.copy(name = "Empty Key Size Doc", balance = 100.0)
                }.getOrThrow()

                val sizeResult = cache.readSizeFromDatabase()
                sizeResult.shouldBeInstanceOf<Success<Long>>()
                sizeResult.getOrThrow().shouldBe(1L)
            }

            it("should verify size with documents having special characters in key") {
                val specialKey = "size-test-key_with.special@chars#123"

                // Create document with special key
                cache.create(specialKey) { doc ->
                    doc.copy(name = "Special Key Size Doc", balance = 200.0)
                }.getOrThrow()

                val sizeResult = cache.readSizeFromDatabase()
                sizeResult.shouldBeInstanceOf<Success<Long>>()
                sizeResult.getOrThrow().shouldBe(1L)
            }

            it("should verify size consistency with readAllFromDatabase") {

                // Create multiple documents
                cache.create("sizeConsistencyKey1") { doc ->
                    doc.copy(name = "Consistency Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("sizeConsistencyKey2") { doc ->
                    doc.copy(name = "Consistency Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("sizeConsistencyKey3") { doc ->
                    doc.copy(name = "Consistency Doc 3", balance = 300.0)
                }.getOrThrow()

                // Size from readSizeFromDatabase
                val sizeResult = cache.readSizeFromDatabase()
                sizeResult.shouldBeInstanceOf<Success<Long>>()
                val size = sizeResult.getOrThrow()

                // Size from readAllFromDatabase
                val allDocs = cache.readAllFromDatabase().getOrThrow().toList()
                val allDocsSize = allDocs.size.toLong()

                // Both should be the same
                size.shouldBe(allDocsSize)
                size.shouldBe(3L)
            }

            it("should verify size consistency with readKeysFromDatabase") {

                // Create multiple documents
                cache.create("sizeKeysConsistencyKey1") { doc ->
                    doc.copy(name = "Keys Consistency Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("sizeKeysConsistencyKey2") { doc ->
                    doc.copy(name = "Keys Consistency Doc 2", balance = 200.0)
                }.getOrThrow()

                // Size from readSizeFromDatabase
                val sizeResult = cache.readSizeFromDatabase()
                sizeResult.shouldBeInstanceOf<Success<Long>>()
                val size = sizeResult.getOrThrow()

                // Size from readKeysFromDatabase
                val keys = cache.readKeysFromDatabase().getOrThrow().toList()
                val keysSize = keys.size.toLong()

                // Both should be the same
                size.shouldBe(keysSize)
                size.shouldBe(2L)
            }

            it("should verify size after multiple operations sequence") {

                // Initial size should be 0
                var sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(0L)

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
                sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(3L)

                // Update one document
                cache.update("sequenceKey2") { doc ->
                    doc.copy(name = "Updated Sequence Doc 2", balance = 250.0)
                }.getOrThrow()

                // Size should still be 3
                sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(3L)

                // Delete one document
                cache.delete("sequenceKey1").getOrThrow()

                // Size should be 2
                sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(2L)

                // Create another document
                cache.create("sequenceKey4") { doc ->
                    doc.copy(name = "Sequence Doc 4", balance = 400.0)
                }.getOrThrow()

                // Size should be 3
                sizeResult = cache.readSizeFromDatabase()
                sizeResult.getOrThrow().shouldBe(3L)
            }
        }
    }
}
