package com.jakemoore.datakache.test.integration.error

import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Reject
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf

@Suppress("unused")
class TestErrorHandling : AbstractDataKacheTest() {

    init {
        describe("Error Handling") {

            it("should handle duplicate document key exception on create") {
                val cache = getCache()
                
                // Create first document
                cache.create("duplicateKeyTest") { doc ->
                    doc.copy(name = "Duplicate Key Test Doc", balance = 100.0)
                }.getOrThrow()
                
                // Try to create document with same key - should fail
                val result = cache.create("duplicateKeyTest") { doc ->
                    doc.copy(name = "Duplicate Key Test Doc 2", balance = 200.0)
                }
                
                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateDocumentKeyException>()
                wrapper.exception.keyString shouldBe "duplicateKeyTest"
                
                // Verify the original document still exists
                val readResult = cache.read("duplicateKeyTest")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe "Duplicate Key Test Doc"
                readResult.getOrThrow().balance shouldBe 100.0
            }

            it("should handle duplicate unique index exception on create") {
                val cache = getCache()
                
                // Create first document with unique name and balance
                cache.create("uniqueIndexTest1") { doc ->
                    doc.copy(name = "Unique Index Test Doc", balance = 100.0)
                }.getOrThrow()
                
                // Try to create document with same name (unique index violation)
                val result = cache.create("uniqueIndexTest2") { doc ->
                    doc.copy(name = "Unique Index Test Doc", balance = 200.0)
                }
                
                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                // Verify the original document still exists
                val readResult = cache.read("uniqueIndexTest1")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe "Unique Index Test Doc"
                readResult.getOrThrow().balance shouldBe 100.0
            }

            it("should handle document not found exception on update") {
                val cache = getCache()
                
                // Try to update non-existent document
                val result = cache.update("nonExistentUpdateKey") { doc ->
                    doc.copy(name = "Updated Non Existent Doc", balance = 200.0)
                }
                
                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DocumentNotFoundException>()
                wrapper.exception.keyString shouldBe "nonExistentUpdateKey"
            }

            it("should handle document not found exception on delete") {
                val cache = getCache()
                
                // Try to delete non-existent document
                val result = cache.delete("nonExistentDeleteKey")
                
                result.shouldBeInstanceOf<Success<Boolean>>()
                result.getOrThrow().shouldBe(false) // Delete returns false when document not found
            }

            it("should handle rejectable update with RejectUpdateException") {
                val cache = getCache()
                
                // Create a document
                cache.create("rejectableUpdateKey") { doc ->
                    doc.copy(name = "Rejectable Update Doc", balance = 100.0)
                }.getOrThrow()
                
                // Try to update with rejection logic
                val result = cache.updateRejectable("rejectableUpdateKey") { doc ->
                    throw RejectUpdateException("Balance too low for update")
                }
                
                result.shouldBeInstanceOf<Reject<TestGenericDoc>>()
                
                // Verify the document was not updated
                val readResult = cache.read("rejectableUpdateKey")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe "Rejectable Update Doc"
                readResult.getOrThrow().balance shouldBe 100.0
            }

            it("should handle successful rejectable update") {
                val cache = getCache()
                
                // Create a document
                cache.create("successfulRejectableKey") { doc ->
                    doc.copy(name = "Successful Rejectable Doc", balance = 100.0)
                }.getOrThrow()
                
                // Update with sufficient balance (should succeed)
                val result = cache.updateRejectable("successfulRejectableKey") { doc ->
                    // Only reject if balance is too low
                    if (doc.balance < 50.0) {
                        throw RejectUpdateException("Balance too low for update")
                    }
                    doc.copy(name = "Updated Successful Rejectable Doc", balance = 200.0)
                }
                
                result.shouldBeInstanceOf<Success<TestGenericDoc>>()
                result.getOrThrow().name shouldBe "Updated Successful Rejectable Doc"
                result.getOrThrow().balance shouldBe 200.0
            }

            it("should handle document not found in rejectable update") {
                val cache = getCache()
                
                // Try to update non-existent document with rejectable update
                val result = cache.updateRejectable("nonExistentRejectableKey") { doc ->
                    doc.copy(name = "Updated Non Existent Rejectable Doc", balance = 200.0)
                }
                
                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DocumentNotFoundException>()
                wrapper.exception.keyString shouldBe "nonExistentRejectableKey"
            }

            it("should handle duplicate key exception on update with unique constraints") {
                val cache = getCache()
                
                // Create two documents with different names
                cache.create("updateDuplicateKey1") { doc ->
                    doc.copy(name = "Update Duplicate Key Doc 1", balance = 100.0)
                }.getOrThrow()
                
                cache.create("updateDuplicateKey2") { doc ->
                    doc.copy(name = "Update Duplicate Key Doc 2", balance = 200.0)
                }.getOrThrow()
                
                // Try to update second document to have same name as first (unique index violation)
                val result = cache.update("updateDuplicateKey2") { doc ->
                    doc.copy(name = "Update Duplicate Key Doc 1", balance = 300.0)
                }
                
                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = result.exception
                System.err.println("Exception Class: ${wrapper.exception::class.java}")
                System.err.println("Exception Message: ${wrapper.exception.message}")

                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                // Verify the original documents still exist unchanged
                val readResult1 = cache.read("updateDuplicateKey1")
                readResult1.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult1.getOrThrow().name shouldBe "Update Duplicate Key Doc 1"
                
                val readResult2 = cache.read("updateDuplicateKey2")
                readResult2.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult2.getOrThrow().name shouldBe "Update Duplicate Key Doc 2"
            }

            it("should handle readOrCreate with existing document") {
                val cache = getCache()
                
                // Create a document
                cache.create("readOrCreateExistingKey") { doc ->
                    doc.copy(name = "ReadOrCreate Existing Doc", balance = 100.0)
                }.getOrThrow()
                
                // Try to readOrCreate with same key - should return existing document
                val result = cache.readOrCreate("readOrCreateExistingKey") { doc ->
                    doc.copy(name = "ReadOrCreate New Doc", balance = 200.0)
                }
                
                result.shouldBeInstanceOf<Success<TestGenericDoc>>()
                result.getOrThrow().name shouldBe "ReadOrCreate Existing Doc"
                result.getOrThrow().balance shouldBe 100.0
            }

            it("should handle readOrCreate with non-existent document") {
                val cache = getCache()
                
                // Try to readOrCreate non-existent document
                val result = cache.readOrCreate("readOrCreateNewKey") { doc ->
                    doc.copy(name = "ReadOrCreate New Doc", balance = 100.0)
                }
                
                result.shouldBeInstanceOf<Success<TestGenericDoc>>()
                result.getOrThrow().name shouldBe "ReadOrCreate New Doc"
                result.getOrThrow().balance shouldBe 100.0
            }

            it("should handle complex error scenarios with multiple operations") {
                val cache = getCache()
                
                // Create initial documents
                cache.create("complexErrorKey1") { doc ->
                    doc.copy(name = "Complex Error Doc 1", balance = 100.0)
                }.getOrThrow()
                
                cache.create("complexErrorKey2") { doc ->
                    doc.copy(name = "Complex Error Doc 2", balance = 200.0)
                }.getOrThrow()
                
                // Try to create duplicate key
                val createResult = cache.create("complexErrorKey1") { doc ->
                    doc.copy(name = "Duplicate Complex Error Doc", balance = 300.0)
                }
                createResult.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                
                // Try to update non-existent document
                val updateResult = cache.update("nonExistentComplexKey") { doc ->
                    doc.copy(name = "Updated Non Existent Complex Doc", balance = 400.0)
                }
                updateResult.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                
                // Try to update with rejection
                val rejectableResult = cache.updateRejectable("complexErrorKey1") { doc ->
                    if (doc.balance < 150.0) {
                        throw RejectUpdateException("Balance too low for complex error test")
                    }
                    doc.copy(name = "Updated Complex Error Doc", balance = 500.0)
                }
                rejectableResult.shouldBeInstanceOf<Reject<TestGenericDoc>>()
                
                // Verify original documents are unchanged
                val readResult1 = cache.read("complexErrorKey1")
                readResult1.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult1.getOrThrow().name shouldBe "Complex Error Doc 1"
                readResult1.getOrThrow().balance shouldBe 100.0
                
                val readResult2 = cache.read("complexErrorKey2")
                readResult2.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult2.getOrThrow().name shouldBe "Complex Error Doc 2"
                readResult2.getOrThrow().balance shouldBe 200.0
            }

            it("should handle error scenarios with empty string keys") {
                val cache = getCache()
                
                // Create document with empty key
                cache.create("") { doc ->
                    doc.copy(name = "Empty Key Error Doc", balance = 100.0)
                }.getOrThrow()
                
                // Try to create another document with empty key
                val result = cache.create("") { doc ->
                    doc.copy(name = "Duplicate Empty Key Error Doc", balance = 200.0)
                }
                
                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateDocumentKeyException>()
                wrapper.exception.keyString shouldBe ""

                // Verify original document exists
                val readResult = cache.read("")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe "Empty Key Error Doc"
            }

            it("should handle error scenarios with special character keys") {
                val cache = getCache()
                val specialKey = "error-test-key_with.special@chars#123"
                
                // Create document with special key
                cache.create(specialKey) { doc ->
                    doc.copy(name = "Special Key Error Doc", balance = 100.0)
                }.getOrThrow()
                
                // Try to create another document with same special key
                val result = cache.create(specialKey) { doc ->
                    doc.copy(name = "Duplicate Special Key Error Doc", balance = 200.0)
                }
                
                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateDocumentKeyException>()
                wrapper.exception.keyString shouldBe specialKey

                // Verify original document exists
                val readResult = cache.read(specialKey)
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe "Special Key Error Doc"
            }

            it("should handle error scenarios with null values") {
                val cache = getCache()
                
                // Create document with null values
                cache.create("nullValueErrorKey") { doc ->
                    doc.copy(
                        name = null,
                        balance = 0.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }.getOrThrow()
                
                // Try to create another document with same name (null) - should fail due to unique index
                val result = cache.create("nullValueErrorKey2") { doc ->
                    doc.copy(
                        name = null,
                        balance = 100.0
                    )
                }
                
                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                // Verify original document exists
                val readResult = cache.read("nullValueErrorKey")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name.shouldBe(null)
            }

            it("should handle error scenarios with complex data") {
                val cache = getCache()
                
                // Create document with complex data
                cache.create("complexErrorKey") { doc ->
                    doc.copy(
                        name = "Complex Error Doc",
                        balance = 100.0,
                        list = listOf("item1", "item2"),
                        customList = listOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customSet = setOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customMap = mapOf("key1" to com.jakemoore.datakache.util.doc.data.MyData.createSample())
                    )
                }.getOrThrow()
                
                // Try to update non-existent document with complex data
                val result = cache.update("nonExistentComplexErrorKey") { doc ->
                    doc.copy(
                        name = "Updated Complex Error Doc",
                        balance = 200.0,
                        list = listOf("item3", "item4"),
                        customList = listOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customSet = setOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customMap = mapOf("key2" to com.jakemoore.datakache.util.doc.data.MyData.createSample())
                    )
                }
                
                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DocumentNotFoundException>()
                wrapper.exception.keyString shouldBe "nonExistentComplexErrorKey"

                // Verify original document exists unchanged
                val readResult = cache.read("complexErrorKey")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe "Complex Error Doc"
                readResult.getOrThrow().list shouldBe listOf("item1", "item2")
            }

            it("should handle error scenarios with random key creation") {
                val cache = getCache()
                
                // Create document with random key
                val randomDoc = cache.createRandom { doc ->
                    doc.copy(name = "Random Key Error Doc", balance = 100.0)
                }.getOrThrow()
                
                // Try to create another document with same random key
                val result = cache.create(randomDoc.key) { doc ->
                    doc.copy(name = "Duplicate Random Key Error Doc", balance = 200.0)
                }
                
                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper: ResultExceptionWrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateDocumentKeyException>()
                wrapper.exception.keyString shouldBe randomDoc.key

                // Verify original document exists
                val readResult = cache.read(randomDoc.key)
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe "Random Key Error Doc"
            }
        }
    }
}
