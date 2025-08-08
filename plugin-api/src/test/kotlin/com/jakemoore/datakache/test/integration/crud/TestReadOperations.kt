package com.jakemoore.datakache.test.integration.crud

import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestPlayerDoc
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldNotBeInstanceOf

@Suppress("unused")
class TestReadOperations : AbstractDataKacheTest() {

    init {
        describe("Read Operations") {

            it("should read online player") {
                val cache = getCache()
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
        }
    }
}
