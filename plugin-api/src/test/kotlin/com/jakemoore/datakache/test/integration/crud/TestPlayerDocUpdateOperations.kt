package com.jakemoore.datakache.test.integration.crud

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
                val cache = getCache()
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
        }
    }
}
