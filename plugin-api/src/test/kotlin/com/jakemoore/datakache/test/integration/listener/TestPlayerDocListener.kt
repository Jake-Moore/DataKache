package com.jakemoore.datakache.test.integration.listener

import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestPlayerDoc
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.mockbukkit.mockbukkit.entity.PlayerMock
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

@Suppress("unused")
class TestPlayerDocListener : AbstractDataKacheTest() {

    init {
        describe("Test PlayerDoc Listener") {
            it("should set PlayerDoc username on first join") {
                val username = "LateJoinPlayer"
                val player = addPlayer(username)

                // Verify that the database now contains the updated PlayerDoc
                eventually(5.seconds) {
                    val dbResult = cache.readFromDatabase(player.uniqueId)
                    dbResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                    val dbDoc = dbResult.value
                    dbDoc.uniqueId.shouldBe(player.uniqueId)
                    dbDoc.username.shouldBe(username) // this should have been set during player join!
                }

                // Verify that the cache now contains the updated PlayerDoc
                eventually(5.seconds) {
                    val result = cache.read(player).shouldNotBeNull()
                    result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                    val doc = result.value
                    doc.uniqueId.shouldBe(player.uniqueId)
                    doc.username.shouldBe(username) // this should have been set during player join!
                }
            }

            it("should set PlayerDoc username even if previously created") {
                val username = "LateJoinPlayer"
                val uuid = UUID.randomUUID()

                // Simulate a player doc being created before that player joins
                cache.create(uuid) {
                    it.copy(
                        name = "InitialName",
                    )
                }.getOrThrow()

                // Simulate that the player joins later
                val player = PlayerMock(server, username, uuid)
                server.addPlayer(player)

                // Verify that the database now contains the updated PlayerDoc
                eventually(5.seconds) {
                    val dbResult = cache.readFromDatabase(player.uniqueId)
                    dbResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                    val dbDoc = dbResult.value
                    dbDoc.uniqueId.shouldBe(uuid)
                    dbDoc.name.shouldBe("InitialName")
                    dbDoc.username.shouldBe(username) // this should have been updated during player join!
                }

                // Verify that the cache now contains the updated PlayerDoc
                eventually(5.seconds) {
                    val result = cache.read(player).shouldNotBeNull()
                    result.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                    val doc = result.value
                    doc.uniqueId.shouldBe(uuid)
                    doc.name.shouldBe("InitialName")
                    doc.username.shouldBe(username) // this should have been updated during player join!
                }
            }
        }
    }
}
