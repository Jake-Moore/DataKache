package com.jakemoore.datakache.test.integration.crud

import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldNotBeInstanceOf

@Suppress("unused")
class TestReadOrCreateOperations : AbstractDataKacheTest() {

    init {
        describe("ReadOrCreate Operations") {

            it("should read existing document") {
                val cache = getCache()

                // Create a document first
                val originalDoc = cache.create("readOrCreateExistingKey") { doc ->
                    doc.copy(
                        name = "Existing Document",
                        balance = 150.0
                    )
                }.getOrThrow()

                // Use readOrCreate - should read existing document
                val result = cache.readOrCreate("readOrCreateExistingKey") { doc ->
                    doc.copy(
                        name = "This should not be used",
                        balance = 999.0
                    )
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()
                result.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val readDoc = result.getOrThrow()
                readDoc.key shouldBe "readOrCreateExistingKey"
                readDoc.name shouldBe "Existing Document" // Should not use initializer
                readDoc.balance shouldBe 150.0 // Should not use initializer
                readDoc.version shouldBe originalDoc.version
            }

            it("should create document when it doesn't exist") {
                val cache = getCache()

                // Use readOrCreate on non-existent document
                val result = cache.readOrCreate("readOrCreateNewKey") { doc ->
                    doc.copy(
                        name = "New Document Created",
                        balance = 250.0
                    )
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()
                result.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe "readOrCreateNewKey"
                createdDoc.name shouldBe "New Document Created"
                createdDoc.balance shouldBe 250.0
                createdDoc.version shouldBe 0L

                // Verify document is now in cache
                cache.isCached("readOrCreateNewKey").shouldBe(true)
                cache.read("readOrCreateNewKey").shouldBeInstanceOf<Success<TestGenericDoc>>()
            }

            it("should create document with default initializer when it doesn't exist") {
                val cache = getCache()

                // Use readOrCreate with default initializer
                val result = cache.readOrCreate("readOrCreateDefaultKey")

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()
                result.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe "readOrCreateDefaultKey"
                createdDoc.name shouldBe null
                createdDoc.balance shouldBe 0.0
                createdDoc.list shouldBe emptyList()
                createdDoc.version shouldBe 0L

                // Verify document is now in cache
                cache.isCached("readOrCreateDefaultKey").shouldBe(true)
            }

            it("should create document with complex data when it doesn't exist") {
                val cache = getCache()

                // Use readOrCreate with complex initializer
                val result = cache.readOrCreate("readOrCreateComplexKey") { doc ->
                    doc.copy(
                        name = "Complex Created Document",
                        balance = 500.0,
                        list = listOf("item1", "item2", "item3"),
                        customList = listOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customSet = setOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customMap = mapOf("key1" to com.jakemoore.datakache.util.doc.data.MyData.createSample())
                    )
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe "readOrCreateComplexKey"
                createdDoc.name shouldBe "Complex Created Document"
                createdDoc.balance shouldBe 500.0
                createdDoc.list shouldBe listOf("item1", "item2", "item3")
                createdDoc.customList.size shouldBe 1
                createdDoc.customSet.size shouldBe 1
                createdDoc.customMap.size shouldBe 1
                createdDoc.version shouldBe 0L
            }

            it("should handle multiple readOrCreate operations") {
                val cache = getCache()

                // Create multiple documents using readOrCreate
                val doc1 = cache.readOrCreate("multiKey1") { doc ->
                    doc.copy(name = "Document 1", balance = 100.0)
                }.getOrThrow()

                val doc2 = cache.readOrCreate("multiKey2") { doc ->
                    doc.copy(name = "Document 2", balance = 200.0)
                }.getOrThrow()

                val doc3 = cache.readOrCreate("multiKey3") { doc ->
                    doc.copy(name = "Document 3", balance = 300.0)
                }.getOrThrow()

                // Verify all documents were created
                doc1.key shouldBe "multiKey1"
                doc1.name shouldBe "Document 1"
                doc1.balance shouldBe 100.0

                doc2.key shouldBe "multiKey2"
                doc2.name shouldBe "Document 2"
                doc2.balance shouldBe 200.0

                doc3.key shouldBe "multiKey3"
                doc3.name shouldBe "Document 3"
                doc3.balance shouldBe 300.0

                // Verify all documents are in cache
                cache.getCacheSize().shouldBe(3)
                cache.isCached("multiKey1").shouldBe(true)
                cache.isCached("multiKey2").shouldBe(true)
                cache.isCached("multiKey3").shouldBe(true)
            }

            it("should read existing document after creation") {
                val cache = getCache()

                // Create document using readOrCreate
                val createdDoc = cache.readOrCreate("readAfterCreateKey") { doc ->
                    doc.copy(name = "Created Document", balance = 100.0)
                }.getOrThrow()

                // Read the same document again using readOrCreate
                val readResult = cache.readOrCreate("readAfterCreateKey") { doc ->
                    doc.copy(name = "This should not be used", balance = 999.0)
                }

                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val readDoc = readResult.getOrThrow()
                readDoc.key shouldBe "readAfterCreateKey"
                readDoc.name shouldBe "Created Document" // Should not use initializer
                readDoc.balance shouldBe 100.0 // Should not use initializer
                readDoc.version shouldBe createdDoc.version
            }

            it("should handle readOrCreate with empty string key") {
                val cache = getCache()

                // Use readOrCreate with empty key
                val result = cache.readOrCreate("") { doc ->
                    doc.copy(name = "Empty Key Document")
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe ""
                createdDoc.name shouldBe "Empty Key Document"
                createdDoc.version shouldBe 0L

                // Verify document is in cache
                cache.isCached("").shouldBe(true)
            }

            it("should handle readOrCreate with special characters in key") {
                val cache = getCache()
                val specialKey = "test-key_with.special@chars#123"

                // Use readOrCreate with special key
                val result = cache.readOrCreate(specialKey) { doc ->
                    doc.copy(name = "Special Key Document")
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe specialKey
                createdDoc.name shouldBe "Special Key Document"
                createdDoc.version shouldBe 0L

                // Verify document is in cache
                cache.isCached(specialKey).shouldBe(true)
            }

            it("should handle readOrCreate with null values") {
                val cache = getCache()

                // Use readOrCreate with null values
                val result = cache.readOrCreate("nullReadOrCreateKey") { doc ->
                    doc.copy(
                        name = null,
                        balance = 0.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe "nullReadOrCreateKey"
                createdDoc.name shouldBe null
                createdDoc.balance shouldBe 0.0
                createdDoc.list shouldBe emptyList()
                createdDoc.customList shouldBe emptyList()
                createdDoc.customSet shouldBe emptySet()
                createdDoc.customMap shouldBe emptyMap()
                createdDoc.version shouldBe 0L
            }

            it("should handle readOrCreate after document update") {
                val cache = getCache()

                // Create document using readOrCreate
                val originalDoc = cache.readOrCreate("updateReadOrCreateKey") { doc ->
                    doc.copy(name = "Original", balance = 100.0)
                }.getOrThrow()

                // Update the document
                cache.update("updateReadOrCreateKey") { doc ->
                    doc.copy(name = "Updated", balance = 200.0)
                }.getOrThrow()

                // Use readOrCreate again - should read the updated document
                val readResult = cache.readOrCreate("updateReadOrCreateKey") { doc ->
                    doc.copy(name = "This should not be used", balance = 999.0)
                }

                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val readDoc = readResult.getOrThrow()
                readDoc.key shouldBe "updateReadOrCreateKey"
                readDoc.name shouldBe "Updated" // Should not use initializer
                readDoc.balance shouldBe 200.0 // Should not use initializer
                readDoc.version shouldBe (originalDoc.version + 1)
            }

            it("should handle readOrCreate after document deletion") {
                val cache = getCache()

                // Create document using readOrCreate
                cache.readOrCreate("deleteReadOrCreateKey") { doc ->
                    doc.copy(name = "Original", balance = 100.0)
                }.getOrThrow()

                // Delete the document
                cache.delete("deleteReadOrCreateKey").getOrThrow()

                // Use readOrCreate again - should create new document
                val result = cache.readOrCreate("deleteReadOrCreateKey") { doc ->
                    doc.copy(name = "Recreated", balance = 300.0)
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe "deleteReadOrCreateKey"
                createdDoc.name shouldBe "Recreated"
                createdDoc.balance shouldBe 300.0
                createdDoc.version shouldBe 0L // New document, so version 0
            }
        }
    }
}
