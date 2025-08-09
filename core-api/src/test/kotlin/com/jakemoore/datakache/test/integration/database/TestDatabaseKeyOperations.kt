package com.jakemoore.datakache.test.integration.database

import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldNotBeInstanceOf
import kotlinx.coroutines.flow.toList

@Suppress("unused")
class TestDatabaseKeyOperations : AbstractDataKacheTest() {

    init {
        describe("Database Key Operations") {

            it("should read keys from empty database") {
                val keysResult = cache.readKeysFromDatabase()

                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()
                keysResult.shouldNotBeInstanceOf<Empty<kotlinx.coroutines.flow.Flow<String>>>()
                keysResult.shouldNotBeInstanceOf<Failure<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.shouldBe(emptyList())
            }

            it("should read keys from database with single document") {

                // Create one document
                cache.create("singleKeyTest") { doc ->
                    doc.copy(name = "Single Key Test", balance = 100.0)
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 1
                keys shouldBe listOf("singleKeyTest")
            }

            it("should read keys from database with multiple documents") {

                // Create multiple documents
                cache.create("multiKey1") { doc ->
                    doc.copy(name = "Multi Key Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("multiKey2") { doc ->
                    doc.copy(name = "Multi Key Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("multiKey3") { doc ->
                    doc.copy(name = "Multi Key Doc 3", balance = 300.0)
                }.getOrThrow()

                cache.create("multiKey4") { doc ->
                    doc.copy(name = "Multi Key Doc 4", balance = 400.0)
                }.getOrThrow()

                cache.create("multiKey5") { doc ->
                    doc.copy(name = "Multi Key Doc 5", balance = 500.0)
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 5
                keys.toSet() shouldBe setOf("multiKey1", "multiKey2", "multiKey3", "multiKey4", "multiKey5")
            }

            it("should read keys from database with documents having complex data") {

                val myData = com.jakemoore.datakache.util.doc.data.MyData.createSample()

                // Create documents with complex data
                cache.create("complexKey1") { doc ->
                    doc.copy(
                        name = "Complex Key Doc 1",
                        balance = 100.0,
                        list = listOf("item1", "item2"),
                        customList = listOf(myData),
                        customSet = setOf(myData),
                        customMap = mapOf("key1" to myData)
                    )
                }.getOrThrow()

                cache.create("complexKey2") { doc ->
                    doc.copy(
                        name = "Complex Key Doc 2",
                        balance = 200.0,
                        list = listOf("item3", "item4"),
                        customList = listOf(myData),
                        customSet = setOf(myData),
                        customMap = mapOf("key2" to myData)
                    )
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 2
                keys.toSet() shouldBe setOf("complexKey1", "complexKey2")
            }

            it("should read keys from database with empty string key") {

                // Create document with empty string key
                cache.create("") { doc ->
                    doc.copy(name = "Empty String Key Doc", balance = 100.0)
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 1
                keys shouldBe listOf("")
            }

            it("should read keys from database with special character keys") {

                val specialKey1 = "special-key_with.dots@123"
                val specialKey2 = "another_special_key#456"
                val specialKey3 = "key with spaces and symbols!@#$%^&*()"

                // Create documents with special character keys
                cache.create(specialKey1) { doc ->
                    doc.copy(name = "Special Key Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create(specialKey2) { doc ->
                    doc.copy(name = "Special Key Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create(specialKey3) { doc ->
                    doc.copy(name = "Special Key Doc 3", balance = 300.0)
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 3
                keys.toSet() shouldBe setOf(specialKey1, specialKey2, specialKey3)
            }

            it("should read keys from database with very long keys") {

                // Must stay under MongoDB’s 1024-byte limit for the implicit _id index
                val longKey1 = "a".repeat(256)
                val longKey2 = "b".repeat(512)
                val longKey3 = "c".repeat(1000)

                // Create documents with very long keys
                cache.create(longKey1) { doc ->
                    doc.copy(name = "Long Key Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create(longKey2) { doc ->
                    doc.copy(name = "Long Key Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create(longKey3) { doc ->
                    doc.copy(name = "Long Key Doc 3", balance = 300.0)
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 3
                keys.toSet() shouldBe setOf(longKey1, longKey2, longKey3)
            }

            it("should read keys from database with numeric string keys") {

                // Create documents with numeric string keys
                cache.create("123") { doc ->
                    doc.copy(name = "Numeric Key Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("456") { doc ->
                    doc.copy(name = "Numeric Key Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("789") { doc ->
                    doc.copy(name = "Numeric Key Doc 3", balance = 300.0)
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 3
                keys.toSet() shouldBe setOf("123", "456", "789")
            }

            it("should read keys from database with unicode keys") {

                val unicodeKey1 = "key_with_unicode_éñç"
                val unicodeKey2 = "key_with_emoji_🎉🎊🎈"
                val unicodeKey3 = "key_with_chinese_中文"
                val unicodeKey4 = "key_with_japanese_日本語"

                // Create documents with Unicode keys
                cache.create(unicodeKey1) { doc ->
                    doc.copy(name = "Unicode Key Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create(unicodeKey2) { doc ->
                    doc.copy(name = "Unicode Key Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create(unicodeKey3) { doc ->
                    doc.copy(name = "Unicode Key Doc 3", balance = 300.0)
                }.getOrThrow()

                cache.create(unicodeKey4) { doc ->
                    doc.copy(name = "Unicode Key Doc 4", balance = 400.0)
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 4
                keys.toSet() shouldBe setOf(unicodeKey1, unicodeKey2, unicodeKey3, unicodeKey4)
            }

            it("should read keys from database after document deletion") {

                // Create multiple documents
                cache.create("deleteKey1") { doc ->
                    doc.copy(name = "Delete Key Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("deleteKey2") { doc ->
                    doc.copy(name = "Delete Key Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("deleteKey3") { doc ->
                    doc.copy(name = "Delete Key Doc 3", balance = 300.0)
                }.getOrThrow()

                // Verify all keys exist initially
                val initialKeysResult = cache.readKeysFromDatabase()
                initialKeysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val initialKeysFlow = initialKeysResult.getOrThrow()
                val initialKeys = initialKeysFlow.toList()
                initialKeys.size shouldBe 3
                initialKeys.toSet() shouldBe setOf("deleteKey1", "deleteKey2", "deleteKey3")

                // Delete one document
                cache.delete("deleteKey2").getOrThrow()

                // Verify keys after deletion
                val finalKeysResult = cache.readKeysFromDatabase()
                finalKeysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val finalKeysFlow = finalKeysResult.getOrThrow()
                val finalKeys = finalKeysFlow.toList()
                finalKeys.size shouldBe 2
                finalKeys.toSet() shouldBe setOf("deleteKey1", "deleteKey3")
            }

            it("should read keys from database after document updates") {

                // Create documents
                cache.create("updateKey1") { doc ->
                    doc.copy(name = "Update Key Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("updateKey2") { doc ->
                    doc.copy(name = "Update Key Doc 2", balance = 200.0)
                }.getOrThrow()

                // Verify initial keys
                val initialKeysResult = cache.readKeysFromDatabase()
                initialKeysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val initialKeysFlow = initialKeysResult.getOrThrow()
                val initialKeys = initialKeysFlow.toList()
                initialKeys.size shouldBe 2
                initialKeys.toSet() shouldBe setOf("updateKey1", "updateKey2")

                // Update one document
                cache.update("updateKey1") { doc ->
                    doc.copy(name = "Updated Key Doc 1", balance = 150.0)
                }.getOrThrow()

                // Verify keys after update (should remain the same)
                val finalKeysResult = cache.readKeysFromDatabase()
                finalKeysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val finalKeysFlow = finalKeysResult.getOrThrow()
                val finalKeys = finalKeysFlow.toList()
                finalKeys.size shouldBe 2
                finalKeys.toSet() shouldBe setOf("updateKey1", "updateKey2")
            }

            it("should read keys from database with documents having extreme balance values") {

                // Create documents with extreme balance values
                cache.create("extremeKey1") { doc ->
                    doc.copy(name = "Extreme Key Doc 1", balance = Double.MAX_VALUE)
                }.getOrThrow()

                cache.create("extremeKey2") { doc ->
                    doc.copy(name = "Extreme Key Doc 2", balance = Double.MIN_VALUE)
                }.getOrThrow()

                cache.create("extremeKey3") { doc ->
                    doc.copy(name = "Extreme Key Doc 3", balance = Double.NEGATIVE_INFINITY)
                }.getOrThrow()

                cache.create("extremeKey4") { doc ->
                    doc.copy(name = "Extreme Key Doc 4", balance = Double.POSITIVE_INFINITY)
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 4
                keys.toSet() shouldBe setOf("extremeKey1", "extremeKey2", "extremeKey3", "extremeKey4")
            }

            it("should read keys from database with documents having very long names") {

                val longName = "a".repeat(10000)

                // Create documents with very long names
                cache.create("longNameKey1") { doc ->
                    doc.copy(name = longName, balance = 100.0)
                }.getOrThrow()

                cache.create("longNameKey2") { doc ->
                    doc.copy(name = longName + "_2", balance = 200.0)
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 2
                keys.toSet() shouldBe setOf("longNameKey1", "longNameKey2")
            }

            it("should read keys from database with documents having all null values") {

                // Create documents with all null values
                cache.create("allNullKey1") { doc ->
                    doc.copy(
                        name = null,
                        balance = 0.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }.getOrThrow()

                cache.create("allNullKey2") { doc ->
                    doc.copy(
                        name = "",
                        balance = -1.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 2
                keys.toSet() shouldBe setOf("allNullKey1", "allNullKey2")
            }

            it("should read keys from database with documents having mixed data types") {

                val myData = com.jakemoore.datakache.util.doc.data.MyData.createSample()

                // Create documents with mixed data types
                cache.create("mixedDataKey1") { doc ->
                    doc.copy(
                        name = "Mixed Data Doc 1",
                        balance = 123.456789,
                        list = listOf("string1", "string2", "string3"),
                        customList = listOf(myData),
                        customSet = setOf(myData),
                        customMap = mapOf("key1" to myData, "key2" to myData)
                    )
                }.getOrThrow()

                cache.create("mixedDataKey2") { doc ->
                    doc.copy(
                        name = null,
                        balance = -999.999,
                        list = emptyList(),
                        customList = listOf(myData, myData),
                        customSet = setOf(myData, myData),
                        customMap = emptyMap()
                    )
                }.getOrThrow()

                val keysResult = cache.readKeysFromDatabase()
                keysResult.shouldBeInstanceOf<Success<kotlinx.coroutines.flow.Flow<String>>>()

                val keysFlow = keysResult.getOrThrow()
                val keys = keysFlow.toList()
                keys.size shouldBe 2
                keys.toSet() shouldBe setOf("mixedDataKey1", "mixedDataKey2")
            }
        }
    }
}
