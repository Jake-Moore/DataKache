package com.jakemoore.datakache.test.integration.indexes

import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.exception.data.Operation
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
class TestUniqueIndex : AbstractDataKacheTest() {

    init {
        describe("Unique Index Tests") {

            it("should read document by name index from cache") {

                // Create a document with a specific name
                val createdDoc = cache.create("nameTestKey") { doc ->
                    doc.copy(
                        name = "Unique Name Test",
                        balance = 150.0
                    )
                }.getOrThrow()

                // Read by name index using companion object method
                val readResult = cache.readByName("Unique Name Test")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Empty<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val readDoc = readResult.getOrThrow()
                readDoc.key shouldBe "nameTestKey"
                readDoc.name shouldBe "Unique Name Test"
                readDoc.balance shouldBe 150.0
                readDoc.version shouldBe createdDoc.version
            }

            it("should read document by balance index from cache") {

                // Create a document with a specific balance
                val createdDoc = cache.create("balanceTestKey") { doc ->
                    doc.copy(
                        name = "Balance Test Document",
                        balance = 250.75
                    )
                }.getOrThrow()

                // Read by balance index using companion object method
                val readResult = cache.readByBalance(250.75)
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Empty<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val readDoc = readResult.getOrThrow()
                readDoc.key shouldBe "balanceTestKey"
                readDoc.name shouldBe "Balance Test Document"
                readDoc.balance shouldBe 250.75
                readDoc.version shouldBe createdDoc.version
            }

            it("should return Empty when reading by non-existent name") {

                // Create a document with a different name
                cache.create("existingKey") { doc ->
                    doc.copy(name = "Existing Name", balance = 100.0)
                }.getOrThrow()

                // Try to read by non-existent name
                val readResult = cache.readByName("Non Existent Name")
                readResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()
            }

            it("should return Empty when reading by non-existent balance") {

                // Create a document with a different balance
                cache.create("existingKey") { doc ->
                    doc.copy(name = "Existing Document", balance = 100.0)
                }.getOrThrow()

                // Try to read by non-existent balance
                val readResult = cache.readByBalance(999.99)
                readResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()
            }

            it("should return Empty when reading by null name") {
                val readResult = cache.readByName(null)
                readResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()
            }

            it("should return Empty when reading by null balance") {
                val readResult = cache.readByBalance(null)
                readResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()
            }

            it("should read document by name index from database") {

                // Create a document with a specific name
                val createdDoc = cache.create("dbNameTestKey") { doc ->
                    doc.copy(
                        name = "Database Name Test",
                        balance = 300.0
                    )
                }.getOrThrow()

                // Read by name index from database
                val readResult = cache.readByUniqueIndexFromDatabase(
                    cache.nameField,
                    "Database Name Test"
                )
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Empty<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val readDoc = readResult.getOrThrow()
                readDoc.key shouldBe "dbNameTestKey"
                readDoc.name shouldBe "Database Name Test"
                readDoc.balance shouldBe 300.0
                readDoc.version shouldBe createdDoc.version
            }

            it("should read document by balance index from database") {

                // Create a document with a specific balance
                val createdDoc = cache.create("dbBalanceTestKey") { doc ->
                    doc.copy(
                        name = "Database Balance Test",
                        balance = 450.25
                    )
                }.getOrThrow()

                // Read by balance index from database
                val readResult = cache.readByUniqueIndexFromDatabase(
                    cache.balanceField,
                    450.25
                )
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Empty<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val readDoc = readResult.getOrThrow()
                readDoc.key shouldBe "dbBalanceTestKey"
                readDoc.name shouldBe "Database Balance Test"
                readDoc.balance shouldBe 450.25
                readDoc.version shouldBe createdDoc.version
            }

            it("should return Empty when reading by non-existent name from database") {

                // Create a document with a different name
                cache.create("existingKey") { doc ->
                    doc.copy(name = "Existing Name", balance = 100.0)
                }.getOrThrow()

                // Try to read by non-existent name from database
                val readResult = cache.readByUniqueIndexFromDatabase(
                    cache.nameField,
                    "Non Existent Database Name"
                )
                readResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()
            }

            it("should return Empty when reading by non-existent balance from database") {

                // Create a document with a different balance
                cache.create("existingKey") { doc ->
                    doc.copy(name = "Existing Document", balance = 100.0)
                }.getOrThrow()

                // Try to read by non-existent balance from database
                val readResult = cache.readByUniqueIndexFromDatabase(
                    cache.balanceField,
                    999.99
                )
                readResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Success<TestGenericDoc>>()
                readResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()
            }

            it("should handle multiple documents with different names") {

                // Create multiple documents with different names
                val doc1 = cache.create("key1") { doc ->
                    doc.copy(name = "First Document", balance = 100.0)
                }.getOrThrow()

                val doc2 = cache.create("key2") { doc ->
                    doc.copy(name = "Second Document", balance = 200.0)
                }.getOrThrow()

                val doc3 = cache.create("key3") { doc ->
                    doc.copy(name = "Third Document", balance = 300.0)
                }.getOrThrow()

                // Read each by name
                val read1 = cache.readByName("First Document")
                read1.shouldBeInstanceOf<Success<TestGenericDoc>>()
                read1.getOrThrow().key shouldBe "key1"

                val read2 = cache.readByName("Second Document")
                read2.shouldBeInstanceOf<Success<TestGenericDoc>>()
                read2.getOrThrow().key shouldBe "key2"

                val read3 = cache.readByName("Third Document")
                read3.shouldBeInstanceOf<Success<TestGenericDoc>>()
                read3.getOrThrow().key shouldBe "key3"
            }

            it("should handle multiple documents with different balances") {

                // Create multiple documents with different balances
                val doc1 = cache.create("key1") { doc ->
                    doc.copy(name = "Document 1", balance = 100.0)
                }.getOrThrow()

                val doc2 = cache.create("key2") { doc ->
                    doc.copy(name = "Document 2", balance = 200.0)
                }.getOrThrow()

                val doc3 = cache.create("key3") { doc ->
                    doc.copy(name = "Document 3", balance = 300.0)
                }.getOrThrow()

                // Read each by balance
                val read1 = cache.readByBalance(100.0)
                read1.shouldBeInstanceOf<Success<TestGenericDoc>>()
                read1.getOrThrow().key shouldBe "key1"

                val read2 = cache.readByBalance(200.0)
                read2.shouldBeInstanceOf<Success<TestGenericDoc>>()
                read2.getOrThrow().key shouldBe "key2"

                val read3 = cache.readByBalance(300.0)
                read3.shouldBeInstanceOf<Success<TestGenericDoc>>()
                read3.getOrThrow().key shouldBe "key3"
            }

            it("should handle documents with null name values") {

                // Create a document with null name
                val createdDoc = cache.create("nullNameKey") { doc ->
                    doc.copy(
                        name = null,
                        balance = 150.0
                    )
                }.getOrThrow()

                // Try to read by null name - should return Empty
                val readResult = cache.readByName(null)
                readResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should handle documents with zero balance") {

                // Create a document with zero balance
                cache.create("zeroBalanceKey") { doc ->
                    doc.copy(
                        name = "Zero Balance Document",
                        balance = 0.0
                    )
                }.getOrThrow()

                // Read by zero balance
                val readResult = cache.readByBalance(0.0)
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().key shouldBe "zeroBalanceKey"
                readResult.getOrThrow().balance shouldBe 0.0
            }

            it("should handle documents with negative balance") {

                // Create a document with negative balance
                val createdDoc = cache.create("negativeBalanceKey") { doc ->
                    doc.copy(
                        name = "Negative Balance Document",
                        balance = -50.0
                    )
                }.getOrThrow()

                // Read by negative balance
                val readResult = cache.readByBalance(-50.0)
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().key shouldBe "negativeBalanceKey"
                readResult.getOrThrow().balance shouldBe -50.0
            }

            it("should handle documents with decimal balance precision") {

                // Create a document with precise decimal balance
                val createdDoc = cache.create("preciseBalanceKey") { doc ->
                    doc.copy(
                        name = "Precise Balance Document",
                        balance = 123.456789
                    )
                }.getOrThrow()

                // Read by precise balance
                val readResult = cache.readByBalance(123.456789)
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().key shouldBe "preciseBalanceKey"
                readResult.getOrThrow().balance shouldBe 123.456789
            }

            it("should handle documents with empty string name") {

                // Create a document with empty string name
                val createdDoc = cache.create("emptyNameKey") { doc ->
                    doc.copy(
                        name = "",
                        balance = 100.0
                    )
                }.getOrThrow()

                // Read by empty string name
                val readResult = cache.readByName("")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().key shouldBe "emptyNameKey"
                readResult.getOrThrow().name shouldBe ""
            }

            it("should handle documents with special characters in name") {

                // Create a document with special characters in name
                val specialName = "Test Document with Special Chars: !@#$%^&*()_+-=[]{}|;':\",./<>?"
                val createdDoc = cache.create("specialNameKey") { doc ->
                    doc.copy(
                        name = specialName,
                        balance = 500.0
                    )
                }.getOrThrow()

                // Read by special name
                val readResult = cache.readByName(specialName)
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().key shouldBe "specialNameKey"
                readResult.getOrThrow().name shouldBe specialName
            }

            it("should handle documents with very large balance values") {

                // Create a document with very large balance
                val largeBalance = Double.MAX_VALUE
                val createdDoc = cache.create("largeBalanceKey") { doc ->
                    doc.copy(
                        name = "Large Balance Document",
                        balance = largeBalance
                    )
                }.getOrThrow()

                // Read by large balance
                val readResult = cache.readByBalance(largeBalance)
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().key shouldBe "largeBalanceKey"
                readResult.getOrThrow().balance shouldBe largeBalance
            }

            it("should handle documents with very small balance values") {

                // Create a document with very small balance
                val smallBalance = Double.MIN_VALUE
                val createdDoc = cache.create("smallBalanceKey") { doc ->
                    doc.copy(
                        name = "Small Balance Document",
                        balance = smallBalance
                    )
                }.getOrThrow()

                // Read by small balance
                val readResult = cache.readByBalance(smallBalance)
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().key shouldBe "smallBalanceKey"
                readResult.getOrThrow().balance shouldBe smallBalance
            }

            it("should handle case-insensitive name matching") {

                // Create a document with a specific name
                val createdDoc = cache.create("caseTestKey") { doc ->
                    doc.copy(
                        name = "Case Sensitive Test",
                        balance = 100.0
                    )
                }.getOrThrow()

                // Read by name with different case (should work due to case-insensitive matching)
                val readResult = cache.readByName("case sensitive test")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().key shouldBe "caseTestKey"
                readResult.getOrThrow().name shouldBe "Case Sensitive Test"
            }

            it("should handle documents with complex nested data and unique indexes") {

                val myData = MyData.createRandom()
                val createdDoc = cache.create("complexIndexKey") { doc ->
                    doc.copy(
                        name = "Complex Index Document",
                        balance = 750.0,
                        list = listOf("item1", "item2", "item3"),
                        customList = listOf(myData),
                        customSet = setOf(myData),
                        customMap = mapOf("key1" to myData)
                    )
                }.getOrThrow()

                // Read by name index
                val readByNameResult = cache.readByName("Complex Index Document")
                readByNameResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readByNameResult.getOrThrow().key shouldBe "complexIndexKey"

                // Read by balance index
                val readByBalanceResult = cache.readByBalance(750.0)
                readByBalanceResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readByBalanceResult.getOrThrow().key shouldBe "complexIndexKey"

                // Verify complex data is preserved
                val readDoc = readByNameResult.getOrThrow()
                readDoc.list shouldBe listOf("item1", "item2", "item3")
                readDoc.customList.size shouldBe 1
                readDoc.customSet.size shouldBe 1
                readDoc.customMap.size shouldBe 1
            }

            it("should throw DuplicateUniqueIndexException when creating document with duplicate name") {

                // Create first document with unique name
                val firstDoc = cache.create("uniqueNameTest1") { doc ->
                    doc.copy(name = "Unique Name Test", balance = 100.0)
                }.getOrThrow()

                // Try to create second document with same name - should fail
                val result = cache.create("uniqueNameTest2") { doc ->
                    doc.copy(name = "Unique Name Test", balance = 200.0)
                }

                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                val duplicateException = wrapper.exception
                duplicateException.docCache shouldBe cache
                duplicateException.operation shouldBe Operation.CREATE

                // Verify the original document still exists and is unchanged
                val readResult = cache.read("uniqueNameTest1")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().key shouldBe "uniqueNameTest1"
                readResult.getOrThrow().name shouldBe "Unique Name Test"
                readResult.getOrThrow().balance shouldBe 100.0
                readResult.getOrThrow().version shouldBe firstDoc.version

                // Verify the second document was not created
                val secondReadResult = cache.read("uniqueNameTest2")
                secondReadResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should throw DuplicateUniqueIndexException when creating document with duplicate balance") {

                // Create first document with unique balance
                val firstDoc = cache.create("uniqueBalanceTest1") { doc ->
                    doc.copy(name = "Unique Balance Test 1", balance = 500.0)
                }.getOrThrow()

                // Try to create second document with same balance - should fail
                val result = cache.create("uniqueBalanceTest2") { doc ->
                    doc.copy(name = "Unique Balance Test 2", balance = 500.0)
                }

                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                val duplicateException = wrapper.exception
                duplicateException.docCache shouldBe cache
                duplicateException.operation shouldBe Operation.CREATE

                // Verify the original document still exists and is unchanged
                val readResult = cache.read("uniqueBalanceTest1")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().key shouldBe "uniqueBalanceTest1"
                readResult.getOrThrow().name shouldBe "Unique Balance Test 1"
                readResult.getOrThrow().balance shouldBe 500.0
                readResult.getOrThrow().version shouldBe firstDoc.version

                // Verify the second document was not created
                val secondReadResult = cache.read("uniqueBalanceTest2")
                secondReadResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should throw DuplicateUniqueIndexException when updating document to match existing name") {

                // Create two documents with different names
                val firstDoc = cache.create("updateDuplicateName1") { doc ->
                    doc.copy(name = "Update Duplicate Name 1", balance = 100.0)
                }.getOrThrow()

                val secondDoc = cache.create("updateDuplicateName2") { doc ->
                    doc.copy(name = "Update Duplicate Name 2", balance = 200.0)
                }.getOrThrow()

                // Try to update second document to have same name as first - should fail
                val result = cache.update("updateDuplicateName2") { doc ->
                    doc.copy(name = "Update Duplicate Name 1", balance = 300.0)
                }

                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                val duplicateException = wrapper.exception
                duplicateException.docCache shouldBe cache
                duplicateException.operation shouldBe Operation.UPDATE

                // Verify both documents still exist and are unchanged
                val readResult1 = cache.read("updateDuplicateName1")
                readResult1.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult1.getOrThrow().name shouldBe "Update Duplicate Name 1"
                readResult1.getOrThrow().balance shouldBe 100.0
                readResult1.getOrThrow().version shouldBe firstDoc.version

                val readResult2 = cache.read("updateDuplicateName2")
                readResult2.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult2.getOrThrow().name shouldBe "Update Duplicate Name 2"
                readResult2.getOrThrow().balance shouldBe 200.0
                readResult2.getOrThrow().version shouldBe secondDoc.version
            }

            it("should throw DuplicateUniqueIndexException when updating document to match existing balance") {

                // Create two documents with different balances
                val firstDoc = cache.create("updateDuplicateBalance1") { doc ->
                    doc.copy(name = "Update Duplicate Balance 1", balance = 100.0)
                }.getOrThrow()

                val secondDoc = cache.create("updateDuplicateBalance2") { doc ->
                    doc.copy(name = "Update Duplicate Balance 2", balance = 200.0)
                }.getOrThrow()

                // Try to update second document to have same balance as first - should fail
                val result = cache.update("updateDuplicateBalance2") { doc ->
                    doc.copy(name = "Updated Name", balance = 100.0)
                }

                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                val duplicateException = wrapper.exception
                duplicateException.docCache shouldBe cache
                duplicateException.operation shouldBe Operation.UPDATE

                // Verify both documents still exist and are unchanged
                val readResult1 = cache.read("updateDuplicateBalance1")
                readResult1.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult1.getOrThrow().name shouldBe "Update Duplicate Balance 1"
                readResult1.getOrThrow().balance shouldBe 100.0
                readResult1.getOrThrow().version shouldBe firstDoc.version

                val readResult2 = cache.read("updateDuplicateBalance2")
                readResult2.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult2.getOrThrow().name shouldBe "Update Duplicate Balance 2"
                readResult2.getOrThrow().balance shouldBe 200.0
                readResult2.getOrThrow().version shouldBe secondDoc.version
            }

            it("should handle case-insensitive duplicate name violations") {

                // Create first document with specific name
                val firstDoc = cache.create("caseInsensitiveTest1") { doc ->
                    doc.copy(name = "Case Insensitive Test", balance = 100.0)
                }.getOrThrow()

                // Try to create second document with same name but different case - should be allowed
                val result = cache.create("caseInsensitiveTest2") { doc ->
                    doc.copy(name = "case insensitive test", balance = 200.0)
                }

                result.shouldBeInstanceOf<Success<TestGenericDoc>>()

                // Verify the original document still exists and is unchanged
                val readResult = cache.read("caseInsensitiveTest1")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe "Case Insensitive Test"
                readResult.getOrThrow().balance shouldBe 100.0
                readResult.getOrThrow().version shouldBe firstDoc.version

                // Verify the second document was created and is distinct
                // (case-insensitive read allowed, case-sensitive uniqueness)
                val secondReadResult = cache.read("caseInsensitiveTest2")
                secondReadResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                secondReadResult.getOrThrow().name shouldBe "case insensitive test"
                secondReadResult.getOrThrow().balance shouldBe 200.0
            }

            it("should handle duplicate null name violations") {

                // Create first document with null name
                val firstDoc = cache.create("nullNameTest1") { doc ->
                    doc.copy(name = null, balance = 100.0)
                }.getOrThrow()

                // Try to create second document with null name - should fail
                val result = cache.create("nullNameTest2") { doc ->
                    doc.copy(name = null, balance = 200.0)
                }

                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                val duplicateException = wrapper.exception
                duplicateException.docCache shouldBe cache
                duplicateException.operation shouldBe Operation.CREATE

                // Verify the original document still exists and is unchanged
                val readResult = cache.read("nullNameTest1")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe null
                readResult.getOrThrow().balance shouldBe 100.0
                readResult.getOrThrow().version shouldBe firstDoc.version

                // Verify the second document was not created
                val secondReadResult = cache.read("nullNameTest2")
                secondReadResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should handle duplicate zero balance violations") {

                // Create first document with zero balance
                val firstDoc = cache.create("zeroBalanceTest1") { doc ->
                    doc.copy(name = "Zero Balance Test 1", balance = 0.0)
                }.getOrThrow()

                // Try to create second document with zero balance - should fail
                val result = cache.create("zeroBalanceTest2") { doc ->
                    doc.copy(name = "Zero Balance Test 2", balance = 0.0)
                }

                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                val duplicateException = wrapper.exception
                duplicateException.docCache shouldBe cache
                duplicateException.operation shouldBe Operation.CREATE

                // Verify the original document still exists and is unchanged
                val readResult = cache.read("zeroBalanceTest1")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe "Zero Balance Test 1"
                readResult.getOrThrow().balance shouldBe 0.0
                readResult.getOrThrow().version shouldBe firstDoc.version

                // Verify the second document was not created
                val secondReadResult = cache.read("zeroBalanceTest2")
                secondReadResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should handle complex data with duplicate unique index violations") {

                val myData = MyData.createRandom()

                // Create first document with complex data and unique name
                val firstDoc = cache.create("complexDuplicateTest1") { doc ->
                    doc.copy(
                        name = "Complex Duplicate Test",
                        balance = 100.0,
                        list = listOf("item1", "item2"),
                        customList = listOf(myData),
                        customSet = setOf(myData),
                        customMap = mapOf("key1" to myData)
                    )
                }.getOrThrow()

                // Try to create second document with same name but different complex data - should fail
                val result = cache.create("complexDuplicateTest2") { doc ->
                    doc.copy(
                        name = "Complex Duplicate Test",
                        balance = 200.0,
                        list = listOf("item3", "item4"),
                        customList = listOf(myData),
                        customSet = setOf(myData),
                        customMap = mapOf("key2" to myData)
                    )
                }

                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                val duplicateException = wrapper.exception
                duplicateException.docCache shouldBe cache
                duplicateException.operation shouldBe Operation.CREATE

                // Verify the original document still exists and is unchanged
                val readResult = cache.read("complexDuplicateTest1")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe "Complex Duplicate Test"
                readResult.getOrThrow().balance shouldBe 100.0
                readResult.getOrThrow().list shouldBe listOf("item1", "item2")
                readResult.getOrThrow().version shouldBe firstDoc.version

                // Verify the second document was not created
                val secondReadResult = cache.read("complexDuplicateTest2")
                secondReadResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }

            it("should handle multiple unique index violations in sequence") {

                // Create first document
                val firstDoc = cache.create("sequenceTest1") { doc ->
                    doc.copy(name = "Sequence Test", balance = 100.0)
                }.getOrThrow()

                // Try to create second document with same name - should fail
                val result1 = cache.create("sequenceTest2") { doc ->
                    doc.copy(name = "Sequence Test", balance = 200.0)
                }
                result1.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                result1.exception.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                // Try to create third document with same balance - should fail
                val result2 = cache.create("sequenceTest3") { doc ->
                    doc.copy(name = "Different Sequence Test", balance = 100.0)
                }
                result2.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                result2.exception.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                // Try to create fourth document with different name and balance - should succeed
                val result3 = cache.create("sequenceTest4") { doc ->
                    doc.copy(name = "Different Sequence Test", balance = 300.0)
                }
                result3.shouldBeInstanceOf<Success<TestGenericDoc>>()

                // Verify only the first and fourth documents exist
                val readResult1 = cache.read("sequenceTest1")
                readResult1.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult1.getOrThrow().name shouldBe "Sequence Test"
                readResult1.getOrThrow().balance shouldBe 100.0

                val readResult2 = cache.read("sequenceTest2")
                readResult2.shouldBeInstanceOf<Empty<TestGenericDoc>>()

                val readResult3 = cache.read("sequenceTest3")
                readResult3.shouldBeInstanceOf<Empty<TestGenericDoc>>()

                val readResult4 = cache.read("sequenceTest4")
                readResult4.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult4.getOrThrow().name shouldBe "Different Sequence Test"
                readResult4.getOrThrow().balance shouldBe 300.0
            }

            it("should handle unique index violations with special characters") {

                val specialName = "Special Chars Test: !@#$%^&*()_+-=[]{}|;':\",./<>?"

                // Create first document with special characters in name
                val firstDoc = cache.create("specialCharsTest1") { doc ->
                    doc.copy(name = specialName, balance = 100.0)
                }.getOrThrow()

                // Try to create second document with same special name - should fail
                val result = cache.create("specialCharsTest2") { doc ->
                    doc.copy(name = specialName, balance = 200.0)
                }

                result.shouldBeInstanceOf<Failure<TestGenericDoc>>()
                val wrapper = result.exception
                wrapper.exception.shouldBeInstanceOf<DuplicateUniqueIndexException>()

                val duplicateException = wrapper.exception
                duplicateException.docCache shouldBe cache
                duplicateException.operation shouldBe Operation.CREATE

                // Verify the original document still exists and is unchanged
                val readResult = cache.read("specialCharsTest1")
                readResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                readResult.getOrThrow().name shouldBe specialName
                readResult.getOrThrow().balance shouldBe 100.0
                readResult.getOrThrow().version shouldBe firstDoc.version

                // Verify the second document was not created
                val secondReadResult = cache.read("specialCharsTest2")
                secondReadResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }
        }
    }
}
