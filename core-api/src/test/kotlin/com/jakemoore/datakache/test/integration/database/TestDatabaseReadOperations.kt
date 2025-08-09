package com.jakemoore.datakache.test.integration.database

import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldNotBeInstanceOf
import kotlinx.coroutines.flow.toList

@Suppress("unused")
class TestDatabaseReadOperations : AbstractDataKacheTest() {

    init {
        describe("Database Read Operations") {

            it("should read document from database") {
                // Create a document in cache
                val createdDoc = cache.create("dbReadKey") { doc ->
                    doc.copy(
                        name = "Database Read Document",
                        balance = 150.0
                    )
                }.getOrThrow()

                // Read from database directly
                val dbReadResult = cache.readFromDatabase("dbReadKey")
                dbReadResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                dbReadResult.shouldNotBeInstanceOf<Empty<TestGenericDoc>>()
                dbReadResult.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()

                val dbReadDoc = dbReadResult.getOrThrow()
                dbReadDoc.key shouldBe "dbReadKey"
                dbReadDoc.name shouldBe "Database Read Document"
                dbReadDoc.balance shouldBe 150.0
                dbReadDoc.version shouldBe createdDoc.version
            }

            it("should return Empty for non-existent document in database") {
                val result = cache.readFromDatabase("nonExistentDbKey")

                result.shouldBeInstanceOf<Empty<TestGenericDoc>>()
                result.shouldNotBeInstanceOf<Success<TestGenericDoc>>()
                result.shouldNotBeInstanceOf<Failure<TestGenericDoc>>()
            }

            it("should read all documents from database") {

                // Create multiple documents with unique names and balances
                cache.create("dbReadAllKey1") { doc ->
                    doc.copy(name = "Database Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("dbReadAllKey2") { doc ->
                    doc.copy(name = "Database Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("dbReadAllKey3") { doc ->
                    doc.copy(name = "Database Doc 3", balance = 300.0)
                }.getOrThrow()

                // Read all documents from database
                val allDocs = cache.readAllFromDatabase().getOrThrow().toList()

                allDocs.size shouldBe 3
                allDocs.map { it.key }.toSet() shouldBe setOf("dbReadAllKey1", "dbReadAllKey2", "dbReadAllKey3")
                allDocs.map { it.name }.toSet() shouldBe setOf("Database Doc 1", "Database Doc 2", "Database Doc 3")
                allDocs.map { it.balance }.toSet() shouldBe setOf(100.0, 200.0, 300.0)
            }

            it("should read all documents from empty database") {
                val allDocs = cache.readAllFromDatabase().getOrThrow().toList()

                allDocs.shouldBe(emptyList())
            }

            it("should read keys from database") {

                // Create multiple documents
                cache.create("dbKeysKey1") { doc ->
                    doc.copy(name = "Keys Doc 1", balance = 150.0)
                }.getOrThrow()

                cache.create("dbKeysKey2") { doc ->
                    doc.copy(name = "Keys Doc 2", balance = 250.0)
                }.getOrThrow()

                cache.create("dbKeysKey3") { doc ->
                    doc.copy(name = "Keys Doc 3", balance = 350.0)
                }.getOrThrow()

                // Read keys from database
                val keys = cache.readKeysFromDatabase().getOrThrow().toList()

                keys.size shouldBe 3
                keys shouldBe setOf("dbKeysKey1", "dbKeysKey2", "dbKeysKey3")
            }

            it("should read keys from empty database") {
                val keys = cache.readKeysFromDatabase().getOrThrow().toList()

                keys.shouldBe(emptySet())
            }

            it("should check if key exists in database") {

                // Check non-existent key
                val result1 = cache.hasKeyInDatabase("nonExistentDbKey")
                result1.shouldBeInstanceOf<Success<Boolean>>()
                result1.value shouldBe false

                // Create a document
                cache.create("dbHasKeyTest") { doc ->
                    doc.copy(name = "Has Key Test", balance = 500.0)
                }.getOrThrow()

                // Check existing key
                val result2 = cache.hasKeyInDatabase("dbHasKeyTest")
                result2.shouldBeInstanceOf<Success<Boolean>>()
                result2.value shouldBe true
            }

            it("should read document with complex data from database") {

                val createdDoc = cache.create("dbComplexKey") { doc ->
                    doc.copy(
                        name = "Complex Database Doc",
                        balance = 750.0,
                        list = listOf("db_item1", "db_item2", "db_item3"),
                        customList = listOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customSet = setOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customMap = mapOf("db_key1" to com.jakemoore.datakache.util.doc.data.MyData.createSample())
                    )
                }.getOrThrow()

                val dbReadResult = cache.readFromDatabase("dbComplexKey")
                dbReadResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val dbReadDoc = dbReadResult.getOrThrow()
                dbReadDoc.key shouldBe "dbComplexKey"
                dbReadDoc.name shouldBe "Complex Database Doc"
                dbReadDoc.balance shouldBe 750.0
                dbReadDoc.list shouldBe listOf("db_item1", "db_item2", "db_item3")
                dbReadDoc.customList.size shouldBe 1
                dbReadDoc.customSet.size shouldBe 1
                dbReadDoc.customMap.size shouldBe 1
            }

            it("should read document with null values from database") {

                val createdDoc = cache.create("dbNullKey") { doc ->
                    doc.copy(
                        name = null,
                        balance = 0.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }.getOrThrow()

                val dbReadResult = cache.readFromDatabase("dbNullKey")
                dbReadResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val dbReadDoc = dbReadResult.getOrThrow()
                dbReadDoc.key shouldBe "dbNullKey"
                dbReadDoc.name shouldBe null
                dbReadDoc.balance shouldBe 0.0
                dbReadDoc.list shouldBe emptyList()
                dbReadDoc.customList shouldBe emptyList()
                dbReadDoc.customSet shouldBe emptySet()
                dbReadDoc.customMap shouldBe emptyMap()
            }

            it("should read document with empty string key from database") {

                val createdDoc = cache.create("") { doc ->
                    doc.copy(name = "Empty Key Database Doc", balance = 100.0)
                }.getOrThrow()

                val dbReadResult = cache.readFromDatabase("")
                dbReadResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val dbReadDoc = dbReadResult.getOrThrow()
                dbReadDoc.key shouldBe ""
                dbReadDoc.name shouldBe "Empty Key Database Doc"
                dbReadDoc.balance shouldBe 100.0
            }

            it("should read document with special characters in key from database") {
                val specialKey = "db-test-key_with.special@chars#123"

                val createdDoc = cache.create(specialKey) { doc ->
                    doc.copy(name = "Special Key Database Doc", balance = 200.0)
                }.getOrThrow()

                val dbReadResult = cache.readFromDatabase(specialKey)
                dbReadResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val dbReadDoc = dbReadResult.getOrThrow()
                dbReadDoc.key shouldBe specialKey
                dbReadDoc.name shouldBe "Special Key Database Doc"
                dbReadDoc.balance shouldBe 200.0
            }

            it("should verify database read after cache update") {

                // Create document
                val originalDoc = cache.create("dbUpdateReadKey") { doc ->
                    doc.copy(name = "Original Database Doc", balance = 100.0)
                }.getOrThrow()

                // Update document
                cache.update("dbUpdateReadKey") { doc ->
                    doc.copy(name = "Updated Database Doc", balance = 200.0)
                }.getOrThrow()

                // Read from database - should get updated version
                val dbReadResult = cache.readFromDatabase("dbUpdateReadKey")
                dbReadResult.shouldBeInstanceOf<Success<TestGenericDoc>>()

                val dbReadDoc = dbReadResult.getOrThrow()
                dbReadDoc.key shouldBe "dbUpdateReadKey"
                dbReadDoc.name shouldBe "Updated Database Doc"
                dbReadDoc.balance shouldBe 200.0
                dbReadDoc.version shouldBe (originalDoc.version + 1)
            }

            it("should verify database read after cache delete") {

                // Create document
                cache.create("dbDeleteReadKey") { doc ->
                    doc.copy(name = "Delete Database Doc", balance = 300.0)
                }.getOrThrow()

                // Delete document
                cache.delete("dbDeleteReadKey").getOrThrow()

                // Read from database - should return Empty
                val dbReadResult = cache.readFromDatabase("dbDeleteReadKey")
                dbReadResult.shouldBeInstanceOf<Empty<TestGenericDoc>>()
            }
        }
    }
}
