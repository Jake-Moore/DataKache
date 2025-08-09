package com.jakemoore.datakache.test.integration.crud

import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException
import com.jakemoore.datakache.api.exception.data.Operation
import com.jakemoore.datakache.api.exception.update.IllegalDocumentKeyModificationException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentUsernameModificationException
import com.jakemoore.datakache.api.exception.update.IllegalDocumentVersionModificationException
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestPlayerDoc
import com.jakemoore.datakache.util.doc.data.MyData
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import java.util.UUID

@Suppress("unused")
class TestPlayerDocCreateOperations : AbstractDataKacheTest() {

    init {
        describe("PlayerDocCache Create Operations") {

            it("should create PlayerDoc with valid UUID") {
                val uuid = UUID.randomUUID()

                val result = cache.create(uuid) { doc ->
                    doc.copy(
                        name = "TestPlayer1",
                        balance = 100.0
                    )
                }
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                doc.key.shouldBe(uuid)
                doc.uniqueId.shouldBe(uuid)
                doc.username.shouldBe(null) // Username should be null in create operations
                doc.name.shouldBe("TestPlayer1")
                doc.balance.shouldBe(100.0)
                doc.version.shouldBe(0L)
            }

            it("should create PlayerDoc with custom data") {
                val uuid = UUID.randomUUID()

                val result = cache.create(uuid) { doc ->
                    doc.copy(
                        name = "CustomPlayer",
                        balance = 500.0,
                        list = listOf("item1", "item2", "item3"),
                        customList = listOf(
                            MyData(
                                name = "CustomData",
                                age = 25,
                                list = listOf("custom")
                            )
                        )
                    )
                }
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                doc.key.shouldBe(uuid)
                doc.username.shouldBe(null)
                doc.name.shouldBe("CustomPlayer")
                doc.balance.shouldBe(500.0)
                doc.list.shouldBe(listOf("item1", "item2", "item3"))
                doc.customList.size.shouldBe(1)
                doc.customList.first().name.shouldBe("CustomData")
                doc.version.shouldBe(0L)
            }

            it("should create PlayerDoc with complex data structures") {
                val uuid = UUID.randomUUID()

                val result = cache.create(uuid) { doc ->
                    doc.copy(
                        name = "ComplexPlayer",
                        balance = 1000.0,
                        list = (1..50).map { "item$it" },
                        customList = (1..25).map { index ->
                            MyData(
                                name = "ListData$index",
                                age = index,
                                list = listOf("list$index")
                            )
                        },
                        customSet = (1..10).map { index ->
                            MyData(
                                name = "SetData$index",
                                age = index + 100,
                                list = listOf("set$index")
                            )
                        }.toSet(),
                        customMap = (1..15).associate { index ->
                            "key$index" to MyData(
                                name = "MapData$index",
                                age = index + 200,
                                list = listOf("map$index")
                            )
                        }
                    )
                }
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                doc.key.shouldBe(uuid)
                doc.username.shouldBe(null)
                doc.name.shouldBe("ComplexPlayer")
                doc.balance.shouldBe(1000.0)
                doc.list.size.shouldBe(50)
                doc.customList.size.shouldBe(25)
                doc.customSet.size.shouldBe(10)
                doc.customMap.size.shouldBe(15)
                doc.version.shouldBe(0L)
            }

            it("should create PlayerDoc with null values") {
                val uuid = UUID.randomUUID()

                val result = cache.create(uuid) { doc ->
                    doc.copy(
                        name = null,
                        balance = 0.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                doc.key.shouldBe(uuid)
                doc.username.shouldBe(null)
                doc.name.shouldBe(null)
                doc.balance.shouldBe(0.0)
                doc.list.shouldBe(emptyList())
                doc.customList.shouldBe(emptyList())
                doc.customSet.shouldBe(emptySet())
                doc.customMap.shouldBe(emptyMap())
                doc.version.shouldBe(0L)
            }

            it("should return failure when modifying username") {
                val uuid = UUID.randomUUID()

                val result = cache.create(uuid) { doc ->
                    doc.copyHelper("ModifiedUsername") // This should cause validation failure
                }
                result.shouldBeInstanceOf<Failure<TestPlayerDoc>>()
                val wrapper = result.exception
                val exception = wrapper.exception
                exception.shouldBeInstanceOf<IllegalDocumentUsernameModificationException>()
            }

            it("should return failure when modifying key") {
                val uuid = UUID.randomUUID()
                val differentUuid = UUID.randomUUID()

                val result = cache.create(uuid) { doc ->
                    doc.copy(key = differentUuid) // This should cause validation failure
                }
                result.shouldBeInstanceOf<Failure<TestPlayerDoc>>()
                val wrapper = result.exception
                val exception = wrapper.exception
                exception.shouldBeInstanceOf<IllegalDocumentKeyModificationException>()
            }

            it("should return failure when modifying version") {
                val uuid = UUID.randomUUID()

                val result = cache.create(uuid) { doc ->
                    doc.copy(version = 1L) // This should cause validation failure
                }
                result.shouldBeInstanceOf<Failure<TestPlayerDoc>>()
                val wrapper = result.exception
                val exception = wrapper.exception
                exception.shouldBeInstanceOf<IllegalDocumentVersionModificationException>()
            }

            it("should handle duplicate UUID creation attempts") {
                val uuid = UUID.randomUUID()

                // First creation should succeed
                val result1 = cache.create(uuid) { doc ->
                    doc.copy(name = "FirstPlayer")
                }
                result1.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc1 = result1.value
                doc1.name.shouldBe("FirstPlayer")

                // Second creation with same UUID should fail
                val result2 = cache.create(uuid) { doc ->
                    doc.copy(name = "SecondPlayer")
                }
                result2.shouldBeInstanceOf<Failure<TestPlayerDoc>>()
                val wrapper = result2.exception
                val exception = wrapper.exception
                exception.shouldBeInstanceOf<DuplicateDocumentKeyException>()
                exception.operation.shouldBe(Operation.CREATE)
                exception.keyString.shouldBe(cache.keyToString(uuid))
            }

            it("should handle concurrent creation requests") {
                val uuid = UUID.randomUUID()

                // Perform multiple concurrent creation attempts
                val results = kotlinx.coroutines.coroutineScope {
                    (1..5).map { index ->
                        async(Dispatchers.IO) {
                            delay(index * 30L) // concurrent, but they need to arrive in a specific order
                            cache.create(uuid) { it.copy(name = "ConcurrentPlayer$index") }
                        }
                    }.awaitAll()
                }

                // Only the first should succeed
                results.first().shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val firstDoc = (results.first() as Success<TestPlayerDoc>).value
                firstDoc.key.shouldBe(uuid)
                firstDoc.name.shouldBe("ConcurrentPlayer1")

                // All others should fail with DuplicateDocumentKeyException
                results.drop(1).forEach { result ->
                    result.shouldBeInstanceOf<Failure<TestPlayerDoc>>()
                    val wrapper = result.exception
                    val exception = wrapper.exception
                    exception.shouldBeInstanceOf<DuplicateDocumentKeyException>()
                    exception.operation.shouldBe(Operation.CREATE)
                    exception.keyString.shouldBe(cache.keyToString(uuid))
                }
            }

            it("should handle creation with empty initializer") {
                val uuid = UUID.randomUUID()

                val result = cache.create(uuid) { doc ->
                    doc // No modifications
                }
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                doc.key.shouldBe(uuid)
                doc.username.shouldBe(null)
                doc.name.shouldBe(null) // Default value
                doc.balance.shouldBe(0.0) // Default value
                doc.version.shouldBe(0L)
            }

            it("should handle creation with complex initializer logic") {
                val uuid = UUID.randomUUID()

                val result = cache.create(uuid) { doc ->
                    // Complex logic in initializer
                    val baseDoc = doc.copy(name = "BasePlayer")
                    val withBalance = baseDoc.copy(balance = 750.0)
                    val withList = withBalance.copy(list = listOf("complex", "logic", "test"))
                    withList
                }
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                doc.key.shouldBe(uuid)
                doc.username.shouldBe(null)
                doc.name.shouldBe("BasePlayer")
                doc.balance.shouldBe(750.0)
                doc.list.shouldBe(listOf("complex", "logic", "test"))
                doc.version.shouldBe(0L)
            }

            it("should persist created PlayerDoc to database") {
                val uuid = UUID.randomUUID()

                val result = cache.create(uuid) { doc ->
                    doc.copy(
                        name = "PersistentPlayer",
                        balance = 250.0,
                        list = listOf("persistent", "data")
                    )
                }
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // Verify cache has the document
                doc.name.shouldBe("PersistentPlayer")

                // Read from database to verify persistence
                val readFromDbResult = cache.readFromDatabase(uuid)
                readFromDbResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val dbDoc = readFromDbResult.value

                // Database document should match cache document
                dbDoc.key.shouldBe(uuid)
                dbDoc.username.shouldBe(null)
                dbDoc.name.shouldBe("PersistentPlayer")
                dbDoc.balance.shouldBe(250.0)
                dbDoc.list.shouldBe(listOf("persistent", "data"))
                dbDoc.version.shouldBe(0L)
            }

            it("should create PlayerDoc with special characters in name") {
                val uuid = UUID.randomUUID()
                val specialName = "Player_With_Special_Chars_123!@#$%^&*()"

                val result = cache.create(uuid) { doc ->
                    doc.copy(name = specialName)
                }
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                doc.key.shouldBe(uuid)
                doc.username.shouldBe(null)
                doc.name.shouldBe(specialName)
                doc.version.shouldBe(0L)
            }

            it("should create PlayerDoc with extreme values") {
                val uuid = UUID.randomUUID()

                val result = cache.create(uuid) { doc ->
                    doc.copy(
                        name = "", // Empty string
                        balance = Double.MAX_VALUE,
                        list = (1..1000).map { "item$it" }, // Large list
                        customList = (1..500).map { index ->
                            MyData(
                                name = "ExtremeData$index",
                                age = index,
                                list = (1..100).map { "nested$it" }
                            )
                        }
                    )
                }
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                doc.key.shouldBe(uuid)
                doc.username.shouldBe(null)
                doc.name.shouldBe("")
                doc.balance.shouldBe(Double.MAX_VALUE)
                doc.list.size.shouldBe(1000)
                doc.customList.size.shouldBe(500)
                doc.version.shouldBe(0L)
            }
        }
    }
}
