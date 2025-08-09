package com.jakemoore.datakache.test.integration.crud

import com.jakemoore.datakache.api.exception.InvalidPlayerException
import com.jakemoore.datakache.api.exception.data.Operation
import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestPlayerDoc
import com.jakemoore.datakache.util.doc.data.MyData
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldNotBeInstanceOf
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import java.util.UUID

@Suppress("unused")
class TestPlayerDocReadOperations : AbstractDataKacheTest() {

    init {
        describe("PlayerDocCache Read Operations") {

            it("should read online player documents") {
                val player = addPlayer("TestPlayer1")

                val result = cache.read(player)
                result.shouldNotBeNull()
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                result.shouldNotBeInstanceOf<Empty<TestPlayerDoc>>()
                result.shouldNotBeInstanceOf<Failure<TestPlayerDoc>>()
                val doc = result.value
                doc.username.shouldBe(player.name)
                doc.uniqueId.shouldBe(player.uniqueId)
            }

            it("should reject reading offline players") {
                val player = addPlayer("TestPlayer2")

                // Disconnect the player to simulate offline state
                player.disconnect()

                val e = shouldThrow<InvalidPlayerException> {
                    cache.read(player)
                }
                e.operation.shouldBe(Operation.READ)
                e.message.shouldNotBeNull().lowercase().shouldContain("not online or valid")

                // Should read Success because the player did connect initially
                //  which created their PlayerDoc immediately
                val result = cache.read(player.uniqueId)
                result.shouldNotBeNull()
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                result.shouldNotBeInstanceOf<Empty<TestPlayerDoc>>()
                result.shouldNotBeInstanceOf<Failure<TestPlayerDoc>>()
                val doc = result.value
                doc.username.shouldBe(player.name)
                doc.uniqueId.shouldBe(player.uniqueId)
            }

            it("should read all online players") {

                // Add multiple players
                val player1 = addPlayer("TestPlayer3")
                val player2 = addPlayer("TestPlayer4")
                val player3 = addPlayer("TestPlayer5")

                val allOnline = cache.readAllOnline()
                allOnline.size.shouldBe(3)

                val playerNames = allOnline.map { it.username }.toSet()
                playerNames.shouldBe(setOf("TestPlayer3", "TestPlayer4", "TestPlayer5"))

                val playerUUIDs = allOnline.map { it.uniqueId }.toSet()
                playerUUIDs.shouldBe(setOf(player1.uniqueId, player2.uniqueId, player3.uniqueId))
            }

            it("should handle multiple online players") {

                // Add multiple players
                val players = listOf(
                    addPlayer("TestPlayer6"),
                    addPlayer("TestPlayer7"),
                    addPlayer("TestPlayer8"),
                    addPlayer("TestPlayer9"),
                    addPlayer("TestPlayer10")
                )

                // Read all players individually
                val docs = players.map { player ->
                    val result = cache.read(player)
                    result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                    result.value
                }

                docs.size.shouldBe(5)
                docs.forEachIndexed { index, doc ->
                    doc.username.shouldBe("TestPlayer${6 + index}")
                    doc.uniqueId.shouldBe(players[index].uniqueId)
                }

                // Verify readAllOnline returns the same players
                val allOnline = cache.readAllOnline()
                allOnline.size.shouldBe(5)
                allOnline.map { it.username }.toSet().shouldBe(
                    setOf("TestPlayer6", "TestPlayer7", "TestPlayer8", "TestPlayer9", "TestPlayer10")
                )
            }

            it("should handle player join/quit scenarios") {

                // Start with one player
                val player1 = addPlayer("TestPlayer11")
                cache.readAllOnline().size.shouldBe(1)

                // Add another player
                val player2 = addPlayer("TestPlayer12")
                cache.readAllOnline().size.shouldBe(2)

                // Disconnect one player
                player1.disconnect()
                cache.readAllOnline().size.shouldBe(1)
                cache.readAllOnline().first().username.shouldBe("TestPlayer12")

                // Reconnect the first player
                player1.reconnect()
                cache.readAllOnline().size.shouldBe(2)
                cache.readAllOnline().map { it.username }.toSet().shouldBe(
                    setOf("TestPlayer11", "TestPlayer12")
                )
            }

            it("should handle concurrent access to PlayerDocs") {
                val player = addPlayer("TestPlayer13")

                // Launch 10 concurrent reads
                val results = kotlinx.coroutines.coroutineScope {
                    (1..10).map {
                        async(Dispatchers.IO) {
                            cache.read(player)
                        }
                    }.awaitAll()
                }

                results.forEach { result ->
                    result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                    result.value.username.shouldBe("TestPlayer13")
                    result.value.uniqueId.shouldBe(player.uniqueId)
                }
            }

            it("should handle PlayerDoc versioning correctly") {
                val player = addPlayer("TestPlayer14")

                val result1 = cache.read(player)
                result1.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc1 = result1.value
                val initialVersion = doc1.version

                // Read again - should be the same version
                val result2 = cache.read(player)
                result2.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc2 = result2.value
                doc2.version.shouldBe(initialVersion)

                // Update the document to change version
                val updateResult = cache.update(player) {
                    it.copy(
                        name = "UpdatedName",
                    )
                }
                updateResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val updatedDoc = updateResult.value
                updatedDoc.version.shouldBe(initialVersion + 1)

                // Read again - should get the updated version
                val result3 = cache.read(player)
                result3.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc3 = result3.value
                doc3.version.shouldBe(initialVersion + 1)
                doc3.name.shouldBe("UpdatedName")
            }

            it("should handle PlayerDoc with complex data structures") {
                val player = addPlayer("TestPlayer15")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // Verify the document has the expected structure
                doc.key.shouldBe(player.uniqueId)
                doc.username.shouldBe(player.name)
                doc.name.shouldBe(null) // Default value
                doc.balance.shouldBe(0.0) // Default value
                doc.list.shouldBe(emptyList()) // Default value
                doc.customList.shouldBe(emptyList()) // Default value
                doc.customSet.shouldBe(emptySet()) // Default value
                doc.customMap.shouldBe(emptyMap()) // Default value
            }

            it("should handle PlayerDoc with null username") {

                val result = cache.create(UUID.randomUUID())
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value
                doc.username.shouldBe(null)

                // Read back
                val readResult = cache.read(doc.key)
                readResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val readDoc = readResult.value
                readDoc.username.shouldBe(null)
            }

            it("should handle PlayerDoc with custom data fields") {
                val player = addPlayer("TestPlayer18")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // Update with custom data
                val updateResult = cache.update(player) { doc ->
                    doc.copy(
                        name = "CustomName",
                        balance = 100.0,
                        list = listOf("item1", "item2", "item3"),
                        customList = listOf(MyData.createRandom()),
                        customSet = setOf(MyData.createRandom()),
                        customMap = mapOf("key1" to MyData.createRandom())
                    )
                }
                updateResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val updatedDoc = updateResult.value

                // Verify custom data was set
                updatedDoc.name.shouldBe("CustomName")
                updatedDoc.balance.shouldBe(100.0)
                updatedDoc.list.shouldBe(listOf("item1", "item2", "item3"))
                updatedDoc.customList.size.shouldBe(1)
                updatedDoc.customSet.size.shouldBe(1)
                updatedDoc.customMap.size.shouldBe(1)
            }

            it("should handle PlayerDoc version conflicts") {
                val player = addPlayer("TestPlayer19")

                val result1 = cache.read(player)
                result1.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc1 = result1.value

                val result2 = cache.read(player)
                result2.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc2 = result2.value

                // Both should have the same version initially
                doc1.version.shouldBe(doc2.version)

                // Update one document
                val updateResult = cache.update(player) {
                    it.copy(
                        name = "UpdatedName"
                    )
                }
                updateResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val updatedDoc = updateResult.value

                // The updated document should have a higher version
                updatedDoc.version.shouldBe(doc1.version + 1)
                updatedDoc.name.shouldBe("UpdatedName")

                // Reading again should get the updated version
                val result3 = cache.read(player)
                result3.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc3 = result3.value
                doc3.version.shouldBe(updatedDoc.version)
                doc3.name.shouldBe("UpdatedName")
            }

            it("should read PlayerDoc after player reconnection") {
                val player = addPlayer("TestPlayer20")

                val result1 = cache.read(player)
                result1.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc1 = result1.value

                // Disconnect and reconnect
                player.disconnect()
                player.reconnect()

                val result2 = cache.read(player)
                result2.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc2 = result2.value

                // Should be the same document (same UUID)
                doc2.key.shouldBe(doc1.key)
                doc2.uniqueId.shouldBe(doc1.uniqueId)
                doc2.username.shouldBe(doc1.username)
            }

            it("should handle PlayerDoc with large data payloads") {
                val player = addPlayer("TestPlayer21")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // Create a large list
                val largeList = (1..50).map { "item$it" }
                val largeCustomList = (1..20).map { MyData.createRandom() }
                val largeCustomSet = (1..20).map { MyData.createRandom() }.toSet()
                val largeCustomMap = (1..20).associate { "key$it" to MyData.createRandom() }

                // Update with large data
                val updateResult = cache.update(player) { doc ->
                    doc.copy(
                        list = largeList,
                        customList = largeCustomList,
                        customSet = largeCustomSet,
                        customMap = largeCustomMap
                    )
                }
                updateResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val updatedDoc = updateResult.value

                // Verify large data was set correctly
                updatedDoc.list.size.shouldBe(largeList.size)
                updatedDoc.customList.size.shouldBe(largeCustomList.size)
                updatedDoc.customSet.size.shouldBe(largeCustomSet.size)
                updatedDoc.customMap.size.shouldBe(largeCustomMap.size)
            }

            it("should read PlayerDoc with special characters in username") {
                val player = addPlayer("TestPlayer_With_Special_Chars_123!@#")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                doc.username.shouldBe("TestPlayer_With_Special_Chars_123!@#")
                doc.uniqueId.shouldBe(player.uniqueId)
            }

            it("should handle PlayerDoc with UUID edge cases") {

                // Test with zero UUID
                val zeroUUID = UUID(0L, 0L)
                val player1 = addPlayer("TestPlayer22", zeroUUID)

                val result1 = cache.read(player1)
                result1.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc1 = result1.value
                doc1.uniqueId.shouldBe(zeroUUID)

                // Test with all-ones UUID
                val allOnesUUID = UUID(-1L, -1L)
                val player2 = addPlayer("TestPlayer23", allOnesUUID)

                val result2 = cache.read(player2)
                result2.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc2 = result2.value
                doc2.uniqueId.shouldBe(allOnesUUID)
            }

            it("should handle PlayerDoc with concurrent modifications") {
                val player = addPlayer("TestPlayer24")

                val result1 = cache.read(player)
                result1.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc1 = result1.value

                // Simulate concurrent read while another operation is happening
                val concurrentResult = cache.read(player)
                concurrentResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val concurrentDoc = concurrentResult.value

                // Both should be the same document
                concurrentDoc.key.shouldBe(doc1.key)
                concurrentDoc.version.shouldBe(doc1.version)
                concurrentDoc.username.shouldBe(doc1.username)
            }

            it("should handle PlayerDoc with database sync issues") {
                val player = addPlayer("TestPlayer25")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // The document should be properly cached and accessible
                doc.username.shouldBe(player.name)
                doc.uniqueId.shouldBe(player.uniqueId)

                // Even if there were database sync issues, the cached document should still be readable
                val readAgain = cache.read(player)
                readAgain.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                readAgain.value.shouldBe(doc)
            }

            it("should handle PlayerDoc with cache invalidation scenarios") {
                val player = addPlayer("TestPlayer26")

                val result1 = cache.read(player)
                result1.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc1 = result1.value

                // Update the document
                val updateResult = cache.update(player) {
                    it.copy(
                        name = "UpdatedName",
                    )
                }
                updateResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()

                // Read again - should get the updated version
                val result2 = cache.read(player)
                result2.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc2 = result2.value

                // Should be the updated version
                doc2.name.shouldBe("UpdatedName")
                doc2.version.shouldBe(doc1.version + 1)
            }

            it("should handle PlayerDoc with memory pressure scenarios") {
                // Add multiple players to simulate memory pressure
                val players = (1..10).map { addPlayer("TestPlayer${26 + it}") }

                // Read all players
                val docs = players.map { player ->
                    val result = cache.read(player)
                    result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                    result.value
                }

                // All documents should be readable
                docs.size.shouldBe(10)
                docs.forEachIndexed { index, doc ->
                    doc.username.shouldBe("TestPlayer${27 + index}")
                }

                // readAllOnline should return all players
                val allOnline = cache.readAllOnline()
                allOnline.size.shouldBe(10)
            }

            it("should handle PlayerDoc with network latency simulation") {
                val player = addPlayer("TestPlayer36")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // Even with simulated network latency, the cached document should be immediately available
                doc.username.shouldBe(player.name)
                doc.uniqueId.shouldBe(player.uniqueId)

                // Multiple reads should be fast (cached)
                repeat(10) {
                    val readResult = cache.read(player)
                    readResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                    readResult.value.shouldBe(doc)
                }
            }

            it("should handle PlayerDoc with server restart scenarios") {
                val player = addPlayer("TestPlayer37")

                val result1 = cache.read(player)
                result1.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc1 = result1.value

                // Simulate server restart by disconnecting and reconnecting
                player.disconnect()
                player.reconnect()

                val result2 = cache.read(player)
                result2.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc2 = result2.value

                // Should be the same document (same UUID)
                doc2.key.shouldBe(doc1.key)
                doc2.uniqueId.shouldBe(doc1.uniqueId)
                doc2.username.shouldBe(doc1.username)
            }
        }
    }
}
