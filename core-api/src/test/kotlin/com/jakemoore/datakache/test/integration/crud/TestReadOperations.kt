package com.jakemoore.datakache.test.integration.crud

import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldNotBeInstanceOf

@Suppress("unused")
class TestReadOperations : AbstractDataKacheTest() {

    init {
        describe("Read Operations") {

            it("should read existing document") {
                val cache = getCache()

                // Create a document first
                val createdDoc = cache.create("readTestKey") { doc ->
                    doc.copy(
                        name = "Read Test Document",
                        balance = 150.0
                    )
                }.getOrThrow()

                // Read the document
                val readResult = cache.read("readTestKey")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Empty<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val readDoc = readResult.getOrThrow()
                readDoc.key shouldBe "readTestKey"
                readDoc.name shouldBe "Read Test Document"
                readDoc.balance shouldBe 150.0
                readDoc.version shouldBe createdDoc.version
            }

            it("should return Empty for non-existent document") {
                val cache = getCache()
                val result = cache.read("nonExistentKey")

                result.shouldBeInstanceOf<Empty<TestGenericDoc>>()
                result.shouldNotBeInstanceOf<Success<TestGenericDoc>>()
                result.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()
            }

            it("should read all documents from empty cache") {
                val cache = getCache()
                val allDocs = cache.readAll()

                allDocs.shouldBe(emptyList())
            }

            it("should read all documents from populated cache") {
                val cache = getCache()

                // Create multiple documents
                val doc1 = cache.create("key1") { doc ->
                    doc.copy(name = "Document 1", balance = 100.0)
                }.getOrThrow()

                val doc2 = cache.create("key2") { doc ->
                    doc.copy(name = "Document 2", balance = 200.0)
                }.getOrThrow()

                val doc3 = cache.create("key3") { doc ->
                    doc.copy(name = "Document 3", balance = 300.0)
                }.getOrThrow()

                // Read all documents
                val allDocs = cache.readAll()

                allDocs.size shouldBe 3
                allDocs.map { it.key }.toSet() shouldBe setOf("key1", "key2", "key3")
                allDocs.map { it.name }.toSet() shouldBe setOf("Document 1", "Document 2", "Document 3")
                allDocs.map { it.balance }.toSet() shouldBe setOf(100.0, 200.0, 300.0)
            }

            it("should get all keys from empty cache") {
                val cache = getCache()
                val keys = cache.getKeys()

                keys.shouldBe(emptySet())
            }

            it("should get all keys from populated cache") {
                val cache = getCache()

                // Create multiple documents
                cache.create("key1") { doc -> doc.copy(name = "Document 1", balance = 1.0) }.getOrThrow()
                cache.create("key2") { doc -> doc.copy(name = "Document 2", balance = 2.0) }.getOrThrow()
                cache.create("key3") { doc -> doc.copy(name = "Document 3", balance = 3.0) }.getOrThrow()

                val keys = cache.getKeys()

                keys.size shouldBe 3
                keys shouldBe setOf("key1", "key2", "key3")
            }

            it("should check if key is cached") {
                val cache = getCache()

                // Check non-existent key
                cache.isCached("nonExistentKey").shouldBe(false)

                // Create a document
                cache.create("cachedKey") { doc ->
                    doc.copy(name = "Cached Document")
                }.getOrThrow()

                // Check existing key
                cache.isCached("cachedKey").shouldBe(true)
            }

            it("should get cache size") {
                val cache = getCache()

                // Initial size should be 0
                cache.getCacheSize().shouldBe(0)

                // Create documents
                cache.create("key1") { doc -> doc.copy(name = "Document 1", balance = 1.0) }.getOrThrow()
                cache.getCacheSize().shouldBe(1)

                cache.create("key2") { doc -> doc.copy(name = "Document 2", balance = 2.0) }.getOrThrow()
                cache.getCacheSize().shouldBe(2)

                cache.create("key3") { doc -> doc.copy(name = "Document 3", balance = 3.0) }.getOrThrow()
                cache.getCacheSize().shouldBe(3)
            }

            it("should read document with complex data") {
                val cache = getCache()

                val createdDoc = cache.create("complexKey") { doc ->
                    doc.copy(
                        name = "Complex Document",
                        balance = 500.0,
                        list = listOf("item1", "item2", "item3"),
                        customList = listOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customSet = setOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customMap = mapOf("key1" to com.jakemoore.datakache.util.doc.data.MyData.createSample())
                    )
                }.getOrThrow()

                val readResult = cache.read("complexKey")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val readDoc = readResult.getOrThrow()
                readDoc.key shouldBe "complexKey"
                readDoc.name shouldBe "Complex Document"
                readDoc.balance shouldBe 500.0
                readDoc.list shouldBe listOf("item1", "item2", "item3")
                readDoc.customList.size shouldBe 1
                readDoc.customSet.size shouldBe 1
                readDoc.customMap.size shouldBe 1
            }

            it("should read document with null values") {
                val cache = getCache()

                val createdDoc = cache.create("nullKey") { doc ->
                    doc.copy(
                        name = null,
                        balance = 0.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }.getOrThrow()

                val readResult = cache.read("nullKey")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val readDoc = readResult.getOrThrow()
                readDoc.key shouldBe "nullKey"
                readDoc.name shouldBe null
                readDoc.balance shouldBe 0.0
                readDoc.list shouldBe emptyList()
                readDoc.customList shouldBe emptyList()
                readDoc.customSet shouldBe emptySet()
                readDoc.customMap shouldBe emptyMap()
            }

            it("should read document with empty string key") {
                val cache = getCache()

                val createdDoc = cache.create("") { doc ->
                    doc.copy(name = "Empty Key Document")
                }.getOrThrow()

                val readResult = cache.read("")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val readDoc = readResult.getOrThrow()
                readDoc.key shouldBe ""
                readDoc.name shouldBe "Empty Key Document"
            }
        }
    }
}
