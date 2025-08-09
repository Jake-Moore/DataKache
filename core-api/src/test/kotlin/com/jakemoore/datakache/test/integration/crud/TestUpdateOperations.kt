package com.jakemoore.datakache.test.integration.crud

import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentKeyModificationException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentVersionModificationException
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.exception.update.UpdateFunctionReturnedSameInstanceException
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Reject
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
import com.jakemoore.datakache.util.doc.data.MyData
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldNotBeInstanceOf

@Suppress("unused")
class TestUpdateOperations : AbstractDataKacheTest() {

    init {
        describe("Update Operations") {

            it("should update existing document") {

                // Create a document
                val originalDoc = cache.create("updateKey") { doc ->
                    doc.copy(
                        name = "Original Document",
                        balance = 100.0
                    )
                }.getOrThrow()

                val originalVersion = originalDoc.version

                // Update the document
                val updateResult = cache.update("updateKey") { doc ->
                    doc.copy(
                        name = "Updated Document",
                        balance = 200.0
                    )
                }

                updateResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                updateResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val updatedDoc = updateResult.getOrThrow()
                updatedDoc.key shouldBe "updateKey"
                updatedDoc.name shouldBe "Updated Document"
                updatedDoc.balance shouldBe 200.0
                updatedDoc.version shouldNotBe originalVersion
                updatedDoc.version shouldBe (originalVersion + 1)
            }

            it("should fail when updating non-existent document") {

                val updateResult = cache.update("nonExistentKey") { doc ->
                    doc.copy(name = "This should fail")
                }

                updateResult.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                updateResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()

                val wrapper: ResultExceptionWrapper = updateResult.exception
                wrapper.exception.shouldBeInstanceOf<DocumentNotFoundException>()
                wrapper.exception.keyString shouldBe "nonExistentKey"
            }

            it("should update document with complex transformation") {

                // Create a document with simple data
                val originalDoc = cache.create("complexUpdateKey") { doc ->
                    doc.copy(
                        name = "Simple Document",
                        balance = 50.0,
                        list = emptyList()
                    )
                }.getOrThrow()

                val myData = MyData.createSample()

                // Update with complex data
                val updateResult = cache.update("complexUpdateKey") { doc ->
                    doc.copy(
                        name = "Complex Updated Document",
                        balance = 750.0,
                        list = listOf("new1", "new2", "new3"),
                        customList = listOf(myData),
                        customSet = setOf(myData),
                        customMap = mapOf("complexKey" to myData)
                    )
                }

                updateResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val updatedDoc = updateResult.getOrThrow()
                updatedDoc.key shouldBe "complexUpdateKey"
                updatedDoc.name shouldBe "Complex Updated Document"
                updatedDoc.balance shouldBe 750.0
                updatedDoc.list shouldBe listOf("new1", "new2", "new3")
                updatedDoc.customList shouldBe listOf(myData)
                updatedDoc.customSet shouldBe setOf(myData)
                updatedDoc.customMap shouldBe mapOf("complexKey" to myData)
                updatedDoc.version shouldBe (originalDoc.version + 1)
            }

            it("should update document via document instance") {

                // Create a document
                val originalDoc = cache.create("docInstanceKey") { doc ->
                    doc.copy(name = "Original", balance = 100.0)
                }.getOrThrow()

                // Update via document instance
                val updateResult = cache.update(originalDoc) { doc ->
                    doc.copy(name = "Updated via Instance", balance = 300.0)
                }

                updateResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val updatedDoc = updateResult.getOrThrow()
                updatedDoc.key shouldBe "docInstanceKey"
                updatedDoc.name shouldBe "Updated via Instance"
                updatedDoc.balance shouldBe 300.0
                updatedDoc.version shouldBe (originalDoc.version + 1)
            }

            it("should reject update with RejectUpdateException") {

                // Create a document
                cache.create("rejectKey") { doc ->
                    doc.copy(name = "Original", balance = 100.0)
                }.getOrThrow()

                // Try to update with rejection
                val updateResult = cache.updateRejectable("rejectKey") { doc ->
                    throw RejectUpdateException("Update rejected for testing")
                }

                updateResult.shouldBeInstanceOf<Reject<TestGenericDoc>>()
                updateResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()
                updateResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()
            }

            it("should successfully update with rejectable update") {

                // Create a document
                val originalDoc = cache.create("rejectableKey") { doc ->
                    doc.copy(name = "Original", balance = 100.0)
                }.getOrThrow()

                // Update successfully with rejectable update
                val updateResult = cache.updateRejectable("rejectableKey") { doc ->
                    doc.copy(name = "Updated Successfully", balance = 250.0)
                }

                updateResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                updateResult.shouldNotBeInstanceOf<Reject<TestGenericDoc>>()
                updateResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val updatedDoc = updateResult.getOrThrow()
                updatedDoc.key shouldBe "rejectableKey"
                updatedDoc.name shouldBe "Updated Successfully"
                updatedDoc.balance shouldBe 250.0
                updatedDoc.version shouldBe (originalDoc.version + 1)
            }

            it("should fail rejectable update for non-existent document") {

                val updateResult = cache.updateRejectable("nonExistentRejectKey") { doc ->
                    doc.copy(name = "This should fail")
                }

                updateResult.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                updateResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()
                updateResult.shouldNotBeInstanceOf<Reject<TestGenericDoc>>()

                val wrapper: ResultExceptionWrapper = updateResult.exception
                wrapper.exception.shouldBeInstanceOf<DocumentNotFoundException>()
                wrapper.exception.keyString shouldBe "nonExistentRejectKey"
            }

            it("should update document multiple times") {

                // Create initial document
                val doc1 = cache.create("multiUpdateKey") { doc ->
                    doc.copy(name = "First", balance = 100.0)
                }.getOrThrow()

                // First update
                val doc2 = cache.update("multiUpdateKey") { doc ->
                    doc.copy(name = "Second", balance = 200.0)
                }.getOrThrow()

                // Second update
                val doc3 = cache.update("multiUpdateKey") { doc ->
                    doc.copy(name = "Third", balance = 300.0)
                }.getOrThrow()

                // Third update
                val doc4 = cache.update("multiUpdateKey") { doc ->
                    doc.copy(name = "Fourth", balance = 400.0)
                }.getOrThrow()

                // Verify version progression
                doc1.version shouldBe 0L
                doc2.version shouldBe 1L
                doc3.version shouldBe 2L
                doc4.version shouldBe 3L

                // Verify final state
                doc4.key shouldBe "multiUpdateKey"
                doc4.name shouldBe "Fourth"
                doc4.balance shouldBe 400.0
            }

            it("should update document with null values") {

                // Create document with data
                val originalDoc = cache.create("nullUpdateKey") { doc ->
                    doc.copy(name = "Has Data", balance = 100.0)
                }.getOrThrow()

                // Update with null values
                val updateResult = cache.update("nullUpdateKey") { doc ->
                    doc.copy(
                        name = null,
                        balance = 0.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }

                updateResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val updatedDoc = updateResult.getOrThrow()
                updatedDoc.key shouldBe "nullUpdateKey"
                updatedDoc.name shouldBe null
                updatedDoc.balance shouldBe 0.0
                updatedDoc.list shouldBe emptyList()
                updatedDoc.customList shouldBe emptyList()
                updatedDoc.customSet shouldBe emptySet()
                updatedDoc.customMap shouldBe emptyMap()
                updatedDoc.version shouldBe (originalDoc.version + 1)
            }

            it("should update document with empty string key") {

                // Create document with empty key
                val originalDoc = cache.create("") { doc ->
                    doc.copy(name = "Original Empty Key")
                }.getOrThrow()

                // Update document with empty key
                val updateResult = cache.update("") { doc ->
                    doc.copy(name = "Updated Empty Key")
                }

                updateResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val updatedDoc = updateResult.getOrThrow()
                updatedDoc.key shouldBe ""
                updatedDoc.name shouldBe "Updated Empty Key"
                updatedDoc.version shouldBe (originalDoc.version + 1)
            }

            it("should update document with special characters in key") {
                val specialKey = "test-key_with.special@chars#123"

                // Create document with special key
                val originalDoc = cache.create(specialKey) { doc ->
                    doc.copy(name = "Original Special Key")
                }.getOrThrow()

                // Update document with special key
                val updateResult = cache.update(specialKey) { doc ->
                    doc.copy(name = "Updated Special Key")
                }

                updateResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val updatedDoc = updateResult.getOrThrow()
                updatedDoc.key shouldBe specialKey
                updatedDoc.name shouldBe "Updated Special Key"
                updatedDoc.version shouldBe (originalDoc.version + 1)
            }

            it("should fail when update function returns same instance") {

                // Create a document
                cache.create("sameInstanceKey") { doc ->
                    doc.copy(name = "Original Document", balance = 100.0)
                }.getOrThrow()

                // Try to update with function that returns same instance
                val updateResult = cache.update("sameInstanceKey") { doc ->
                    doc // Return same instance - should fail
                }

                updateResult.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                updateResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()

                val wrapper: ResultExceptionWrapper = updateResult.exception
                wrapper.exception.shouldBeInstanceOf<UpdateFunctionReturnedSameInstanceException>()
                wrapper.exception.docNamespace shouldBe cache.getKeyNamespace("sameInstanceKey")
            }

            it("should fail when update function modifies document key") {

                // Create a document
                cache.create("keyModificationKey") { doc ->
                    doc.copy(name = "Original Document", balance = 100.0)
                }.getOrThrow()

                // Try to update with function that modifies the key
                val updateResult = cache.update("keyModificationKey") { doc ->
                    doc.copy(key = "modifiedKey") // Modify key - should fail
                }

                updateResult.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                updateResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()

                val wrapper: ResultExceptionWrapper = updateResult.exception
                wrapper.exception.shouldBeInstanceOf<IllegalDocumentKeyModificationException>()
                wrapper.exception.docNamespace shouldBe cache.getKeyNamespace("keyModificationKey")
                wrapper.exception.foundKeyString shouldBe "modifiedKey"
                wrapper.exception.expectedKeyString shouldBe "keyModificationKey"
            }

            it("should fail when update function modifies document version") {

                // Create a document
                val originalDoc = cache.create("versionModificationKey") { doc ->
                    doc.copy(name = "Original Document", balance = 100.0)
                }.getOrThrow()

                // Try to update with function that modifies the version
                val updateResult = cache.update("versionModificationKey") { doc ->
                    doc.copy(version = 999L) // Modify version - should fail
                }

                updateResult.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                updateResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()

                val wrapper: ResultExceptionWrapper = updateResult.exception
                wrapper.exception.shouldBeInstanceOf<IllegalDocumentVersionModificationException>()
                wrapper.exception.docNamespace shouldBe cache.getKeyNamespace("versionModificationKey")
                wrapper.exception.foundVersion shouldBe 999L
                wrapper.exception.expectedVersion shouldBe originalDoc.version + 1
            }
        }
    }
}
