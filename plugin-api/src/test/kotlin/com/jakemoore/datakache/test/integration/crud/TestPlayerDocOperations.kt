package com.jakemoore.datakache.test.integration.crud

import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestPlayerDoc
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.mockbukkit.mockbukkit.entity.PlayerMock

@Suppress("unused")
class TestPlayerDocOperations : AbstractDataKacheTest() {

    init {
        describe("PlayerDoc Operations") {

            it("should get online player correctly") {
                val player = addPlayer("TestPlayer1")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                val retrievedPlayer = doc.getPlayer()
                retrievedPlayer.shouldNotBeNull()
                retrievedPlayer.shouldBe(player)
                retrievedPlayer.name.shouldBe("TestPlayer1")
                retrievedPlayer.uniqueId.shouldBe(player.uniqueId)
            }

            it("should return null for offline player") {
                val player = addPlayer("TestPlayer2")

                // Get the PlayerDoc first
                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // Disconnect player to simulate offline
                player.disconnect()

                val retrievedPlayer = doc.getPlayer()
                retrievedPlayer.shouldBeNull()
            }

            it("should handle player state changes") {
                val player = addPlayer("TestPlayer3")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // Initially should be online
                doc.isOnline.shouldBe(true)
                doc.isTrulyOnline.shouldBe(true)

                // Disconnect player to simulate offline
                player.disconnect()

                // Should detect offline state
                doc.isOnline.shouldBe(false)
                doc.isTrulyOnline.shouldBe(false)
            }

            it("should handle username updates via copyHelper") {
                val player = addPlayer("TestPlayer5")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // Test username update
                val updatedDoc = doc.copyHelper("NewUsername")
                updatedDoc.username.shouldBe("NewUsername")
                updatedDoc.key.shouldBe(doc.key)
                updatedDoc.version.shouldBe(doc.version)

                // Test null username
                val nullUsernameDoc = doc.copyHelper(null)
                nullUsernameDoc.username.shouldBeNull()
                nullUsernameDoc.key.shouldBe(doc.key)
                nullUsernameDoc.version.shouldBe(doc.version)

                // Test empty username
                val emptyUsernameDoc = doc.copyHelper("")
                emptyUsernameDoc.username.shouldBe("")
                emptyUsernameDoc.key.shouldBe(doc.key)
                emptyUsernameDoc.version.shouldBe(doc.version)
            }

            it("should handle player reconnection scenarios") {
                val player = addPlayer("TestPlayer6")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // Simulate player disconnect
                player.disconnect()
                doc.getPlayer().shouldBeNull()
                doc.isOnline.shouldBe(false)
                doc.isTrulyOnline.shouldBe(false)

                // Simulate player reconnection
                player.reconnect()

                // The PlayerDoc should now be able to find the reconnected player
                val retrievedPlayer = doc.getPlayer()
                retrievedPlayer.shouldNotBeNull()
                retrievedPlayer.shouldBe(player)
                doc.isOnline.shouldBe(true)
                doc.isTrulyOnline.shouldBe(true)
            }

            it("should handle player name changes") {
                val player = addPlayer("TestPlayer7")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // Update username in the document
                val updatedDoc = doc.copyHelper("ChangedName")
                updatedDoc.username.shouldBe("ChangedName")

                // The player reference should still work
                val retrievedPlayer = updatedDoc.getPlayer()
                retrievedPlayer.shouldNotBeNull()
                retrievedPlayer.shouldBe(player)
            }

            it("should handle UUID consistency") {
                val player = addPlayer("TestPlayer8")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // UUID should be consistent
                doc.uniqueId.shouldBe(player.uniqueId)
                doc.key.shouldBe(player.uniqueId)

                // UUID should remain the same after username changes
                val updatedDoc = doc.copyHelper("NewName")
                updatedDoc.uniqueId.shouldBe(player.uniqueId)
                updatedDoc.key.shouldBe(player.uniqueId)
            }

            it("should handle multiple player state transitions") {
                val player = addPlayer("TestPlayer9")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value

                // Test multiple online/offline cycles
                for (i in 1..3) {
                    // Disconnect player
                    player.disconnect()
                    doc.getPlayer().shouldBeNull()
                    doc.isOnline.shouldBe(false)
                    doc.isTrulyOnline.shouldBe(false)

                    // Reconnect player
                    player.reconnect()

                    doc.getPlayer().shouldNotBeNull()
                    doc.isOnline.shouldBe(true)
                    doc.isTrulyOnline.shouldBe(true)
                }
            }

            it("should handle edge cases with null and invalid players") {
                val player = addPlayer("TestPlayer10")

                val result = cache.read(player)
                result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = result.value
                player.disconnect()

                // Test isOnline and isTrulyOnline with a null player
                val fakePlayer = object : PlayerMock(server, player.name, player.uniqueId) {
                    override fun isOnline(): Boolean = false
                    override fun isValid(): Boolean = false
                }
                doc.initializePlayerInternal(fakePlayer)
                doc.isOnline.shouldBe(false)
                doc.isTrulyOnline.shouldBe(false)
            }
        }
    }
}
