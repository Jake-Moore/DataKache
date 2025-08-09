package com.jakemoore.datakache.test.integration.crud

import com.jakemoore.datakache.api.exception.update.IllegalDocumentUsernameModificationException
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestPlayerDoc
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import java.util.UUID

/**
 * Minimal test to ensure PlayerDoc updates work.
 *
 * PlayerDoc does not change the update behavior from the generic document, and needs no additional tests at this time.
 */
@Suppress("unused")
class TestPlayerDocUpdateOperations : AbstractDataKacheTest() {

    init {
        describe("PlayerDocCache Update Operations") {

            it("should update PlayerDoc with UUID") {
                val uuid = UUID.randomUUID()

                // Create a PlayerDoc first
                val createResult = cache.create(uuid)
                createResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()

                // Update the PlayerDoc
                val updateResult = cache.update(uuid) {
                    it.copy(
                        name = "UpdatedPlayer",
                    )
                }
                updateResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val updatedDoc = updateResult.value

                updatedDoc.key.shouldBe(uuid)
                updatedDoc.name.shouldBe("UpdatedPlayer")
                updatedDoc.version.shouldBe(1L) // Version should be incremented
            }

            it("should return failure when updating username") {
                val player = addPlayer("TestPlayer")

                val readResult = cache.read(player)
                readResult.shouldBeInstanceOf<Success<TestPlayerDoc>>()
                val doc = readResult.value
                doc.shouldBeInstanceOf<TestPlayerDoc>()
                doc.key.shouldBe(player.uniqueId)

                val namespace = cache.getKeyNamespace(doc.key)

                // Trigger username update failure
                val updateResult = doc.update {
                    it.copyHelper(username = "NewUsername")
                }
                updateResult.shouldBeInstanceOf<Failure<TestPlayerDoc>>()
                val wrapper = updateResult.exception
                val exception = wrapper.exception
                exception.shouldBeInstanceOf<IllegalDocumentUsernameModificationException>()
                exception.docNamespace.shouldBe(namespace)
                exception.foundUsername.shouldBe("NewUsername")
                exception.expectedUsername.shouldBe(player.name)
            }
        }
    }
}
