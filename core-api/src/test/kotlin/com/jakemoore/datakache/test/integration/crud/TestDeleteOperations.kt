package com.jakemoore.datakache.test.integration.crud

import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
import com.jakemoore.datakache.util.doc.data.MyData
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldNotBeInstanceOf

@Suppress("unused")
class TestDeleteOperations : AbstractDataKacheTest() {

    init {
        describe("Delete Operations") {

            it("should delete existing document") {

                // Create a document
                cache.create("deleteKey") { doc ->
                    doc.copy(name = "Document to Delete", balance = 100.0)
                }.getOrThrow()

                // Verify document exists
                cache.isCached("deleteKey").shouldBe(true)
                cache.read("deleteKey").shouldBeInstanceOf<Success<TestGenericDoc>>()

                // Delete the document
                val deleteResult = cache.delete("deleteKey")
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()
                deleteResult.shouldNotBeInstanceOf<Failure<Boolean>>()

                val wasDeleted = deleteResult.getOrThrow()
                wasDeleted.shouldBe(true)

                // Verify document is no longer in cache
                cache.isCached("deleteKey").shouldBe(false)
                cache.read("deleteKey").shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should return false when deleting non-existent document") {

                // Verify document doesn't exist
                cache.isCached("nonExistentDeleteKey").shouldBe(false)

                // Try to delete non-existent document
                val deleteResult = cache.delete("nonExistentDeleteKey")
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()
                deleteResult.shouldNotBeInstanceOf<Failure<Boolean>>()

                val wasDeleted = deleteResult.getOrThrow()
                wasDeleted.shouldBe(false)
            }

            it("should delete document via document instance") {

                // Create a document
                val doc = cache.create("docInstanceDeleteKey") { doc ->
                    doc.copy(name = "Document to Delete via Instance", balance = 200.0)
                }.getOrThrow()

                // Verify document exists
                cache.isCached("docInstanceDeleteKey").shouldBe(true)

                // Delete via document instance
                val deleteResult = cache.delete(doc)
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()

                val wasDeleted = deleteResult.getOrThrow()
                wasDeleted.shouldBe(true)

                // Verify document is no longer in cache
                cache.isCached("docInstanceDeleteKey").shouldBe(false)
                cache.read("docInstanceDeleteKey").shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should delete multiple documents") {

                // Create multiple documents
                cache.create("multiDeleteKey1") { doc ->
                    doc.copy(name = "Document 1", balance = 100.0)
                }.getOrThrow()

                cache.create("multiDeleteKey2") { doc ->
                    doc.copy(name = "Document 2", balance = 200.0)
                }.getOrThrow()

                cache.create("multiDeleteKey3") { doc ->
                    doc.copy(name = "Document 3", balance = 300.0)
                }.getOrThrow()

                // Verify all documents exist
                cache.getCacheSize().shouldBe(3)
                cache.isCached("multiDeleteKey1").shouldBe(true)
                cache.isCached("multiDeleteKey2").shouldBe(true)
                cache.isCached("multiDeleteKey3").shouldBe(true)

                // Delete all documents
                val delete1 = cache.delete("multiDeleteKey1").getOrThrow()
                val delete2 = cache.delete("multiDeleteKey2").getOrThrow()
                val delete3 = cache.delete("multiDeleteKey3").getOrThrow()

                delete1.shouldBe(true)
                delete2.shouldBe(true)
                delete3.shouldBe(true)

                // Verify all documents are deleted
                cache.getCacheSize().shouldBe(0)
                cache.isCached("multiDeleteKey1").shouldBe(false)
                cache.isCached("multiDeleteKey2").shouldBe(false)
                cache.isCached("multiDeleteKey3").shouldBe(false)
            }

            it("should delete document with complex data") {

                // Create document with complex data
                cache.create("complexDeleteKey") { doc ->
                    doc.copy(
                        name = "Complex Document",
                        balance = 500.0,
                        list = listOf("item1", "item2", "item3"),
                        customList = listOf(MyData.createRandom()),
                        customSet = setOf(MyData.createRandom()),
                        customMap = mapOf("key1" to MyData.createRandom())
                    )
                }.getOrThrow()

                // Verify document exists
                cache.isCached("complexDeleteKey").shouldBe(true)
                val readResult = cache.read("complexDeleteKey")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                // Delete the document
                val deleteResult = cache.delete("complexDeleteKey")
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()

                val wasDeleted = deleteResult.getOrThrow()
                wasDeleted.shouldBe(true)

                // Verify document is no longer in cache
                cache.isCached("complexDeleteKey").shouldBe(false)
                cache.read("complexDeleteKey").shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should delete document with empty string key") {

                // Create document with empty key
                cache.create("") { doc ->
                    doc.copy(name = "Empty Key Document")
                }.getOrThrow()

                // Verify document exists
                cache.isCached("").shouldBe(true)

                // Delete the document
                val deleteResult = cache.delete("")
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()

                val wasDeleted = deleteResult.getOrThrow()
                wasDeleted.shouldBe(true)

                // Verify document is no longer in cache
                cache.isCached("").shouldBe(false)
                cache.read("").shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should delete document with special characters in key") {
                val specialKey = "test-key_with.special@chars#123"

                // Create document with special key
                cache.create(specialKey) { doc ->
                    doc.copy(name = "Special Key Document")
                }.getOrThrow()

                // Verify document exists
                cache.isCached(specialKey).shouldBe(true)

                // Delete the document
                val deleteResult = cache.delete(specialKey)
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()

                val wasDeleted = deleteResult.getOrThrow()
                wasDeleted.shouldBe(true)

                // Verify document is no longer in cache
                cache.isCached(specialKey).shouldBe(false)
                cache.read(specialKey).shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should handle delete after update") {

                // Create a document
                cache.create("updateThenDeleteKey") { doc ->
                    doc.copy(name = "Original", balance = 100.0)
                }.getOrThrow()

                // Update the document
                cache.update("updateThenDeleteKey") { doc ->
                    doc.copy(name = "Updated", balance = 200.0)
                }.getOrThrow()

                // Verify document exists with updated data
                val readResult = cache.read("updateThenDeleteKey")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                val doc = readResult.getOrThrow()
                doc.name shouldBe "Updated"
                doc.balance shouldBe 200.0

                // Delete the document
                val deleteResult = cache.delete("updateThenDeleteKey")
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()

                val wasDeleted = deleteResult.getOrThrow()
                wasDeleted.shouldBe(true)

                // Verify document is no longer in cache
                cache.isCached("updateThenDeleteKey").shouldBe(false)
                cache.read("updateThenDeleteKey").shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should handle delete of document with null values") {

                // Create document with null values
                cache.create("nullDeleteKey") { doc ->
                    doc.copy(
                        name = null,
                        balance = 0.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }.getOrThrow()

                // Verify document exists
                cache.isCached("nullDeleteKey").shouldBe(true)

                // Delete the document
                val deleteResult = cache.delete("nullDeleteKey")
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()

                val wasDeleted = deleteResult.getOrThrow()
                wasDeleted.shouldBe(true)

                // Verify document is no longer in cache
                cache.isCached("nullDeleteKey").shouldBe(false)
                cache.read("nullDeleteKey").shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should verify cache size after deletions") {

                // Create multiple documents
                cache.create("sizeTestKey1") { doc -> doc.copy(name = "Doc 1", balance = 1.0) }.getOrThrow()
                cache.create("sizeTestKey2") { doc -> doc.copy(name = "Doc 2", balance = 2.0) }.getOrThrow()
                cache.create("sizeTestKey3") { doc -> doc.copy(name = "Doc 3", balance = 3.0) }.getOrThrow()

                // Verify initial size
                cache.getCacheSize().shouldBe(3)
                cache.readSizeFromDatabase().getOrThrow().shouldBe(3)

                // Delete one document
                cache.delete("sizeTestKey1").getOrThrow() shouldBe true
                cache.getCacheSize().shouldBe(2)
                cache.readSizeFromDatabase().getOrThrow().shouldBe(2)

                // Delete another document
                cache.delete("sizeTestKey2").getOrThrow() shouldBe true
                cache.getCacheSize().shouldBe(1)
                cache.readSizeFromDatabase().getOrThrow().shouldBe(1)

                // Delete last document
                cache.delete("sizeTestKey3").getOrThrow() shouldBe true
                cache.getCacheSize().shouldBe(0)
                cache.readSizeFromDatabase().getOrThrow().shouldBe(0)

                // Verify all documents are gone
                cache.read("sizeTestKey1").shouldBeInstanceOf<Empty<TestGenericDoc>>()
                cache.read("sizeTestKey2").shouldBeInstanceOf<Empty<TestGenericDoc>>()
                cache.read("sizeTestKey3").shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }
        }
    }
}
