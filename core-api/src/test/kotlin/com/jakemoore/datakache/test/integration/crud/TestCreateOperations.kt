package com.jakemoore.datakache.test.integration.crud

import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentKeyModificationException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentVersionModificationException
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
import com.jakemoore.datakache.util.doc.data.MyData
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldNotBeInstanceOf
import java.util.UUID

@Suppress("unused")
class TestCreateOperations : AbstractDataKacheTest() {

    init {
        describe("Create Operations") {

            it("should create document with specific key") {
                val result = cache.create("testKey") { doc ->
                    doc.copy(
                        name = "Test Document",
                        balance = 100.0
                    )
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()
                result.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe "testKey"
                createdDoc.name shouldBe "Test Document"
                createdDoc.balance shouldBe 100.0
                createdDoc.version shouldBe 0L
            }

            it("should create document with random key") {
                val result = cache.createRandom { doc ->
                    doc.copy(
                        name = "Random Document",
                        balance = 250.0
                    )
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()
                result.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key.shouldNotBe("testKey") // Should be a random UUID
                kotlin.runCatching { UUID.fromString(createdDoc.key) }.isSuccess shouldBe true
                createdDoc.name shouldBe "Random Document"
                createdDoc.balance shouldBe 250.0
                createdDoc.version shouldBe 0L
            }

            it("should create document with complex nested data") {
                val myData = MyData.createSample()

                val result = cache.create("complexKey") { doc ->
                    doc.copy(
                        name = "Complex Document",
                        balance = 500.0,
                        list = listOf("item1", "item2", "item3"),
                        customList = listOf(myData),
                        customSet = setOf(myData),
                        customMap = mapOf("key1" to myData)
                    )
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe "complexKey"
                createdDoc.name shouldBe "Complex Document"
                createdDoc.balance shouldBe 500.0
                createdDoc.list shouldBe listOf("item1", "item2", "item3")
                createdDoc.customList shouldBe listOf(myData)
                createdDoc.customSet shouldBe setOf(myData)
                createdDoc.customMap shouldBe mapOf("key1" to myData)
            }

            it("should create document with property defaults") {
                val result = cache.create("defaultKey")

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe "defaultKey"
                createdDoc.name shouldBe null
                createdDoc.balance shouldBe 0.0
                createdDoc.list shouldBe emptyList()
                createdDoc.version shouldBe 0L
            }

            it("should fail when creating document with duplicate key") {

                // Create first document
                val firstResult = cache.create("duplicateKey") { doc ->
                    doc.copy(name = "First Document", balance = 1.0)
                }
                firstResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                // Try to create second document with same key
                val secondResult = cache.create("duplicateKey") { doc ->
                    doc.copy(name = "Second Document", balance = 2.0)
                }

                secondResult.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = secondResult.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateDocumentKeyException>()
                wrapper.exception.keyString shouldBe "duplicateKey"
            }

            it("should fail when creating document changes document key") {

                // Create a document
                val createResult = cache.create("keyModificationKey") { doc ->
                    doc.copy(key = "modifiedKey") // Modify key - should fail
                }
                createResult.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                createResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()

                val wrapper: ResultExceptionWrapper = createResult.exception
                wrapper.exception.shouldBeInstanceOf<IllegalDocumentKeyModificationException>()
                wrapper.exception.docNamespace shouldBe cache.getKeyNamespace("keyModificationKey")
                wrapper.exception.foundKeyString shouldBe "modifiedKey"
                wrapper.exception.expectedKeyString shouldBe "keyModificationKey"
            }

            it("should fail when creating document changes document version") {

                // Create a document
                val createResult = cache.create("versionModificationKey") { doc ->
                    doc.copy(version = 42) // Modify version - should fail
                }
                createResult.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                createResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()

                val wrapper: ResultExceptionWrapper = createResult.exception
                wrapper.exception.shouldBeInstanceOf<IllegalDocumentVersionModificationException>()
                wrapper.exception.docNamespace shouldBe cache.getKeyNamespace("versionModificationKey")
                wrapper.exception.foundVersion shouldBe 42
                wrapper.exception.expectedVersion shouldBe 0
            }

            it("should fail when creating document with duplicate unique index") {

                // Create first document with unique key/name, but balance = 1.0
                val firstResult = cache.create("firstDoc") { doc ->
                    doc.copy(name = "First Document", balance = 1.0)
                }
                firstResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                // Create second document with unique key/name, but SAME balance = 1.0
                //   balance is a unique index, and this creation should fail
                val secondResult = cache.create("secondDoc") { doc ->
                    doc.copy(name = "Second Document", balance = 1.0)
                }

                secondResult.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = secondResult.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()
                // MongoDB error message should mention the unique index (balance) and value (1.0)
                wrapper.exception.fullMessage.shouldContain("balance")
            }

            it("should create multiple documents with different keys") {

                val doc1 = cache.create("key1") { doc ->
                    doc.copy(name = "Document 1", balance = 100.0)
                }.getOrThrow()

                val doc2 = cache.create("key2") { doc ->
                    doc.copy(name = "Document 2", balance = 200.0)
                }.getOrThrow()

                val doc3 = cache.create("key3") { doc ->
                    doc.copy(name = "Document 3", balance = 300.0)
                }.getOrThrow()

                // Verify all documents were created successfully
                doc1.key shouldBe "key1"
                doc1.name shouldBe "Document 1"
                doc1.balance shouldBe 100.0

                doc2.key shouldBe "key2"
                doc2.name shouldBe "Document 2"
                doc2.balance shouldBe 200.0

                doc3.key shouldBe "key3"
                doc3.name shouldBe "Document 3"
                doc3.balance shouldBe 300.0

                // Verify all documents are in cache
                cache.read("key1").shouldBeInstanceOf<Success<TestGenericDoc>>()
                cache.read("key2").shouldBeInstanceOf<Success<TestGenericDoc>>()
                cache.read("key3").shouldBeInstanceOf<Success<TestGenericDoc>>()
            }

            it("should create document with empty string key") {
                val result = cache.create("") { doc ->
                    doc.copy(name = "Empty Key Document")
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe ""
                createdDoc.name shouldBe "Empty Key Document"
            }

            it("should create document with special characters in key") {
                val specialKey = "test-key_with.special@chars#123"

                val result = cache.create(specialKey) { doc ->
                    doc.copy(name = "Special Key Document")
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val createdDoc = result.getOrThrow()
                createdDoc.key shouldBe specialKey
                createdDoc.name shouldBe "Special Key Document"
            }
        }
    }
}
