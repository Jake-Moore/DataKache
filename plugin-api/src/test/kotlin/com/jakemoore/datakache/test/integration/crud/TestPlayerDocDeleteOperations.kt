package com.jakemoore.datakache.test.integration.crud

import com.jakemoore.datakache.api.exception.InvalidPlayerException
import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestPlayerDoc
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import java.util.UUID

@Suppress("unused")
class TestPlayerDocDeleteOperations : AbstractDataKacheTest() {

    init {
        describe("PlayerDocCache Delete Operations") {

            it("should clear PlayerDoc with UUID") {
                val uuid = UUID.randomUUID()

                // Create a PlayerDoc with custom data
                val createResult = cache.create(uuid) { doc ->
                    doc.copy(
                        name = "TestPlayer",
                        balance = 100.0,
                        list = listOf("item1", "item2")
                    )
                }
                createResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val originalDoc = createResult.value

                // Verify the document has custom data
                originalDoc.username.shouldBe(null) // Username is null in create operations
                originalDoc.name.shouldBe("TestPlayer")
                originalDoc.balance.shouldBe(100.0)
                originalDoc.list.shouldBe(listOf("item1", "item2"))

                // Clear the PlayerDoc (reset to default state)
                val deleteResult = cache.delete(uuid)
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()
                deleteResult.value.shouldBe(true) // Document was found and cleared

                // Read the document from database
                val dbReadResult = cache.readFromDatabase(uuid)
                dbReadResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val clearedDbDoc = dbReadResult.value
                // Should be reset to default values (UUID preserved, username preserved as null)
                clearedDbDoc.key.shouldBe(uuid)
                clearedDbDoc.username.shouldBe(originalDoc.username) // Username preserved from original
                clearedDbDoc.name.shouldBe(null) // Default value
                clearedDbDoc.balance.shouldBe(0.0) // Default value
                clearedDbDoc.list.shouldBe(emptyList()) // Default value

                // Read the document again - should be reset to default state
                val readResult = cache.read(uuid)
                readResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val clearedDoc = readResult.value
                // Should be reset to default values (UUID preserved, username preserved as null)
                clearedDoc.key.shouldBe(uuid)
                clearedDoc.username.shouldBe(originalDoc.username) // Username preserved from original
                clearedDoc.name.shouldBe(null) // Default value
                clearedDoc.balance.shouldBe(0.0) // Default value
                clearedDoc.list.shouldBe(emptyList()) // Default value
            }

            it("should clear PlayerDoc with Player object") {
                val player = addPlayer("TestPlayer1")

                // First, verify the player document exists
                val initialReadResult = cache.read(player)
                initialReadResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val initialDoc = initialReadResult.value
                initialDoc.username.shouldBe("TestPlayer1")

                // Update it with custom data
                val updateResult = cache.update(player) { doc ->
                    doc.copy(
                        name = "UpdatedPlayer",
                        balance = 500.0,
                        list = listOf("updated1", "updated2", "updated3")
                    )
                }
                updateResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val updatedDoc = updateResult.value
                updatedDoc.name.shouldBe("UpdatedPlayer")
                updatedDoc.balance.shouldBe(500.0)

                // Clear the PlayerDoc using Player object
                val deleteResult = cache.delete(player)
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()
                deleteResult.value.shouldBe(true)

                // Read the document again - should be reset to default state
                val readResult = cache.read(player)
                readResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val clearedDoc = readResult.value

                // Should be reset to default values but preserve username
                clearedDoc.key.shouldBe(player.uniqueId)
                clearedDoc.username.shouldBe("TestPlayer1") // Username should be preserved
                clearedDoc.name.shouldBe(null) // Default value
                clearedDoc.balance.shouldBe(0.0) // Default value
                clearedDoc.list.shouldBe(emptyList()) // Default value
                clearedDoc.version.shouldBe(0L) // Reset version
            }

            it("should handle clearing non-existent PlayerDoc with UUID") {
                val uuid = UUID.randomUUID()

                // Try to clear a PlayerDoc that doesn't exist
                val deleteResult = cache.delete(uuid)
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()
                deleteResult.value.shouldBe(false) // Document was not found

                // Verify the document still doesn't exist
                val readResult = cache.read(uuid)
                readResult.shouldBeInstanceOf<Empty<TestPlayerDoc>>()
            }

            it("should handle clearing offline player") {
                val player = addPlayer("TestPlayer2")

                // Disconnect the player
                player.disconnect()

                // Try to clear the offline player - should throw InvalidPlayerException
                shouldThrow<InvalidPlayerException> {
                    cache.delete(player)
                }
            }

            it("should preserve username when clearing PlayerDoc") {
                val player = addPlayer("PreserveUsername")

                // Create a PlayerDoc with a specific username
                val doc = cache.read(player).getOrThrow()
                doc.username.shouldBe("PreserveUsername")

                // Clear the PlayerDoc
                val deleteResult = cache.delete(player)
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()
                deleteResult.value.shouldBe(true)

                // Read the document again - username should be preserved
                val readResult = cache.read(player)
                readResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val clearedDoc = readResult.value

                clearedDoc.key.shouldBe(player.uniqueId)
                clearedDoc.username.shouldBe("PreserveUsername") // Username should be preserved
                clearedDoc.name.shouldBe(null) // Default value
                clearedDoc.version.shouldBe(0L) // Reset version
            }

            it("should handle multiple clear operations on same PlayerDoc") {
                val player = addPlayer("MultiClearPlayer")

                cache.read(player).shouldBeInstanceOf<Success<TestPlayerDoc>>()

                // Perform multiple clear operations
                val clearResults = (1..5).map { index ->
                    // Update with some data first
                    cache.update(player.uniqueId) { doc ->
                        doc.copy(
                            name = "UpdatedPlayer$index",
                            balance = index * 100.0
                        )
                    }.shouldBeInstanceOf<Success<TestPlayerDoc>>()

                    // Clear the document
                    cache.delete(player.uniqueId)
                }

                clearResults.forEach { result ->
                    result.shouldBeInstanceOf<Success<Boolean>>()
                    result.value.shouldBe(true) // Document should always be found
                }

                // Read the document after all clears
                val readResult = cache.read(player.uniqueId)
                readResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val finalDoc = readResult.value

                // Should be in default state
                finalDoc.key.shouldBe(player.uniqueId)
                finalDoc.username.shouldBe("MultiClearPlayer") // Username preserved
                finalDoc.name.shouldBe(null) // Default value
                finalDoc.balance.shouldBe(0.0) // Default value
                finalDoc.version.shouldBe(0L) // Reset version
            }

            it("should handle clearing PlayerDoc with null username") {
                val uuid = UUID.randomUUID()

                // Create a PlayerDoc with null username
                val createResult = cache.create(uuid)
                createResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val originalDoc = createResult.value
                originalDoc.username.shouldBe(null)

                // Clear the PlayerDoc
                val deleteResult = cache.delete(uuid)
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()
                deleteResult.value.shouldBe(true)

                // Read the document again
                val readResult = cache.read(uuid)
                readResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val clearedDoc = readResult.value

                clearedDoc.key.shouldBe(uuid)
                clearedDoc.username.shouldBe(null) // username should still be null
                clearedDoc.name.shouldBe(null) // Default value
                clearedDoc.version.shouldBe(0L) // Reset version
            }

            it("should persist cleared PlayerDoc to database") {
                val uuid = UUID.randomUUID()

                // Create a PlayerDoc with custom data
                val createResult = cache.create(uuid) { doc ->
                    doc.copy(
                        name = "PersistentPlayer",
                        balance = 750.0,
                        list = listOf("persistent1", "persistent2", "persistent3")
                    )
                }
                createResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val originalDoc = createResult.value

                // Verify data is present
                originalDoc.name.shouldBe("PersistentPlayer")
                originalDoc.balance.shouldBe(750.0)
                originalDoc.list.size.shouldBe(3)

                // Clear the PlayerDoc
                val deleteResult = cache.delete(uuid)
                deleteResult.shouldBeInstanceOf<Success<Boolean>>()
                deleteResult.value.shouldBe(true)

                // Read from database to verify persistence
                val readFromDbResult = cache.readFromDatabase(uuid)
                readFromDbResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val dbDoc = readFromDbResult.value

                // Should be reset to default values in database (UUID and username preserved)
                dbDoc.key.shouldBe(uuid)
                dbDoc.username.shouldBe(originalDoc.username) // Username preserved from original
                dbDoc.name.shouldBe(null) // Default value
                dbDoc.balance.shouldBe(0.0) // Default value
                dbDoc.list.shouldBe(emptyList()) // Default value
                dbDoc.version.shouldBe(0L) // Reset version
            }
        }
    }
}
