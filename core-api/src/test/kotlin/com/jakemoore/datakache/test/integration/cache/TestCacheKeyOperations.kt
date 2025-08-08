package com.jakemoore.datakache.test.integration.cache

import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import io.kotest.matchers.shouldBe

@Suppress("unused")
class TestCacheKeyOperations : AbstractDataKacheTest() {

    init {
        describe("Cache Key Operations") {

            it("should get keys from empty cache") {
                val cache = getCache()
                val keys = cache.getKeys()

                keys.shouldBe(emptySet())
            }

            it("should get keys from cache with one document") {
                val cache = getCache()

                // Create one document
                cache.create("cacheKeysKey1") { doc ->
                    doc.copy(name = "Cache Keys Doc 1", balance = 100.0)
                }.getOrThrow()

                val keys = cache.getKeys()
                keys.size shouldBe 1
                keys shouldBe setOf("cacheKeysKey1")
            }

            it("should get keys from cache with multiple documents") {
                val cache = getCache()

                // Create multiple documents with unique names and balances
                cache.create("cacheKeysKey1") { doc ->
                    doc.copy(name = "Cache Keys Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("cacheKeysKey2") { doc ->
                    doc.copy(name = "Cache Keys Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("cacheKeysKey3") { doc ->
                    doc.copy(name = "Cache Keys Doc 3", balance = 300.0)
                }.getOrThrow()

                cache.create("cacheKeysKey4") { doc ->
                    doc.copy(name = "Cache Keys Doc 4", balance = 400.0)
                }.getOrThrow()

                cache.create("cacheKeysKey5") { doc ->
                    doc.copy(name = "Cache Keys Doc 5", balance = 500.0)
                }.getOrThrow()

                val keys = cache.getKeys()
                keys.size shouldBe 5
                keys shouldBe setOf("cacheKeysKey1", "cacheKeysKey2", "cacheKeysKey3", "cacheKeysKey4", "cacheKeysKey5")
            }

            it("should check if key is cached for non-existent key") {
                val cache = getCache()

                cache.isCached("nonExistentCacheKey").shouldBe(false)
            }

            it("should check if key is cached for existing key") {
                val cache = getCache()

                // Create a document
                cache.create("cachedKeyTest") { doc ->
                    doc.copy(name = "Cached Key Test Doc", balance = 100.0)
                }.getOrThrow()

                cache.isCached("cachedKeyTest").shouldBe(true)
            }

            it("should check if key is cached for multiple keys") {
                val cache = getCache()

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

                // Check all keys
                cache.isCached("multiKey1").shouldBe(true)
                cache.isCached("multiKey2").shouldBe(true)
                cache.isCached("multiKey3").shouldBe(true)

                // Check non-existent key
                cache.isCached("nonExistentMultiKey").shouldBe(false)
            }

            it("should verify keys after creating documents") {
                val cache = getCache()

                // Initial keys should be empty
                cache.getKeys().shouldBe(emptySet())

                // Create first document
                cache.create("keysAfterCreateKey1") { doc ->
                    doc.copy(name = "Keys After Create Doc 1", balance = 100.0)
                }.getOrThrow()

                var keys = cache.getKeys()
                keys.size shouldBe 1
                keys shouldBe setOf("keysAfterCreateKey1")

                // Create second document
                cache.create("keysAfterCreateKey2") { doc ->
                    doc.copy(name = "Keys After Create Doc 2", balance = 200.0)
                }.getOrThrow()

                keys = cache.getKeys()
                keys.size shouldBe 2
                keys shouldBe setOf("keysAfterCreateKey1", "keysAfterCreateKey2")

                // Create third document
                cache.create("keysAfterCreateKey3") { doc ->
                    doc.copy(name = "Keys After Create Doc 3", balance = 300.0)
                }.getOrThrow()

                keys = cache.getKeys()
                keys.size shouldBe 3
                keys shouldBe setOf("keysAfterCreateKey1", "keysAfterCreateKey2", "keysAfterCreateKey3")
            }

            it("should verify keys remain same after updating documents") {
                val cache = getCache()

                // Create a document
                cache.create("keysAfterUpdateKey") { doc ->
                    doc.copy(name = "Keys After Update Doc", balance = 100.0)
                }.getOrThrow()

                var keys = cache.getKeys()
                keys.size shouldBe 1
                keys shouldBe setOf("keysAfterUpdateKey")

                // Update the document
                cache.update("keysAfterUpdateKey") { doc ->
                    doc.copy(name = "Updated Keys After Update Doc", balance = 200.0)
                }.getOrThrow()

                keys = cache.getKeys()
                keys.size shouldBe 1
                keys shouldBe setOf("keysAfterUpdateKey")
            }

            it("should verify keys decrease after deleting documents") {
                val cache = getCache()

                // Create multiple documents
                cache.create("keysAfterDeleteKey1") { doc ->
                    doc.copy(name = "Keys After Delete Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("keysAfterDeleteKey2") { doc ->
                    doc.copy(name = "Keys After Delete Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("keysAfterDeleteKey3") { doc ->
                    doc.copy(name = "Keys After Delete Doc 3", balance = 300.0)
                }.getOrThrow()

                // Initial keys
                var keys = cache.getKeys()
                keys.size shouldBe 3
                keys shouldBe setOf("keysAfterDeleteKey1", "keysAfterDeleteKey2", "keysAfterDeleteKey3")

                // Delete first document
                cache.delete("keysAfterDeleteKey1").getOrThrow()
                keys = cache.getKeys()
                keys.size shouldBe 2
                keys shouldBe setOf("keysAfterDeleteKey2", "keysAfterDeleteKey3")

                // Delete second document
                cache.delete("keysAfterDeleteKey2").getOrThrow()
                keys = cache.getKeys()
                keys.size shouldBe 1
                keys shouldBe setOf("keysAfterDeleteKey3")

                // Delete third document
                cache.delete("keysAfterDeleteKey3").getOrThrow()
                keys = cache.getKeys()
                keys.size shouldBe 0
                keys.shouldBe(emptySet())
            }

            it("should verify keys with documents having complex data") {
                val cache = getCache()

                // Create documents with complex data
                cache.create("keysComplexKey1") { doc ->
                    doc.copy(
                        name = "Complex Keys Doc 1",
                        balance = 100.0,
                        list = listOf("item1", "item2"),
                        customList = listOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customSet = setOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customMap = mapOf("key1" to com.jakemoore.datakache.util.doc.data.MyData.createSample())
                    )
                }.getOrThrow()

                cache.create("keysComplexKey2") { doc ->
                    doc.copy(
                        name = "Complex Keys Doc 2",
                        balance = 200.0,
                        list = listOf("item3", "item4", "item5"),
                        customList = listOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customSet = setOf(com.jakemoore.datakache.util.doc.data.MyData.createSample()),
                        customMap = mapOf("key2" to com.jakemoore.datakache.util.doc.data.MyData.createSample())
                    )
                }.getOrThrow()

                val keys = cache.getKeys()
                keys.size shouldBe 2
                keys shouldBe setOf("keysComplexKey1", "keysComplexKey2")
            }

            it("should verify keys with documents having null values") {
                val cache = getCache()

                // Create document with null values
                cache.create("keysNullKey") { doc ->
                    doc.copy(
                        name = null,
                        balance = 0.0,
                        list = emptyList(),
                        customList = emptyList(),
                        customSet = emptySet(),
                        customMap = emptyMap()
                    )
                }.getOrThrow()

                val keys = cache.getKeys()
                keys.size shouldBe 1
                keys shouldBe setOf("keysNullKey")
            }

            it("should verify keys with documents having empty string key") {
                val cache = getCache()

                // Create document with empty key
                cache.create("") { doc ->
                    doc.copy(name = "Empty Key Keys Doc", balance = 100.0)
                }.getOrThrow()

                val keys = cache.getKeys()
                keys.size shouldBe 1
                keys shouldBe setOf("")
            }

            it("should verify keys with documents having special characters in key") {
                val cache = getCache()
                val specialKey = "keys-test-key_with.special@chars#123"

                // Create document with special key
                cache.create(specialKey) { doc ->
                    doc.copy(name = "Special Key Keys Doc", balance = 200.0)
                }.getOrThrow()

                val keys = cache.getKeys()
                keys.size shouldBe 1
                keys shouldBe setOf(specialKey)
            }

            it("should verify keys consistency with getCacheSize") {
                val cache = getCache()

                // Create multiple documents
                cache.create("keysConsistencyKey1") { doc ->
                    doc.copy(name = "Keys Consistency Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("keysConsistencyKey2") { doc ->
                    doc.copy(name = "Keys Consistency Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("keysConsistencyKey3") { doc ->
                    doc.copy(name = "Keys Consistency Doc 3", balance = 300.0)
                }.getOrThrow()

                // Keys from getKeys
                val keys = cache.getKeys()
                val keysSize = keys.size

                // Size from getCacheSize
                val cacheSize = cache.getCacheSize()

                // Both should be the same
                keysSize.shouldBe(cacheSize)
                keysSize.shouldBe(3)
            }

            it("should verify keys consistency with readAll") {
                val cache = getCache()

                // Create multiple documents
                cache.create("keysReadAllConsistencyKey1") { doc ->
                    doc.copy(name = "Keys ReadAll Consistency Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("keysReadAllConsistencyKey2") { doc ->
                    doc.copy(name = "Keys ReadAll Consistency Doc 2", balance = 200.0)
                }.getOrThrow()

                // Keys from getKeys
                val keys = cache.getKeys()
                val keysSize = keys.size

                // Size from readAll
                val allDocs = cache.readAll()
                val allDocsSize = allDocs.size

                // Both should be the same
                keysSize.shouldBe(allDocsSize)
                keysSize.shouldBe(2)
            }

            it("should verify keys after multiple operations sequence") {
                val cache = getCache()

                // Initial keys should be empty
                cache.getKeys().shouldBe(emptySet())

                // Create documents
                cache.create("sequenceKeysKey1") { doc ->
                    doc.copy(name = "Sequence Keys Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("sequenceKeysKey2") { doc ->
                    doc.copy(name = "Sequence Keys Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("sequenceKeysKey3") { doc ->
                    doc.copy(name = "Sequence Keys Doc 3", balance = 300.0)
                }.getOrThrow()

                // Keys should be 3
                var keys = cache.getKeys()
                keys.size shouldBe 3
                keys shouldBe setOf("sequenceKeysKey1", "sequenceKeysKey2", "sequenceKeysKey3")

                // Update one document
                cache.update("sequenceKeysKey2") { doc ->
                    doc.copy(name = "Updated Sequence Keys Doc 2", balance = 250.0)
                }.getOrThrow()

                // Keys should still be 3
                keys = cache.getKeys()
                keys.size shouldBe 3
                keys shouldBe setOf("sequenceKeysKey1", "sequenceKeysKey2", "sequenceKeysKey3")

                // Delete one document
                cache.delete("sequenceKeysKey1").getOrThrow()

                // Keys should be 2
                keys = cache.getKeys()
                keys.size shouldBe 2
                keys shouldBe setOf("sequenceKeysKey2", "sequenceKeysKey3")

                // Create another document
                cache.create("sequenceKeysKey4") { doc ->
                    doc.copy(name = "Sequence Keys Doc 4", balance = 400.0)
                }.getOrThrow()

                // Keys should be 3
                keys = cache.getKeys()
                keys.size shouldBe 3
                keys shouldBe setOf("sequenceKeysKey2", "sequenceKeysKey3", "sequenceKeysKey4")
            }

            it("should verify keys with readOrCreate operations") {
                val cache = getCache()

                // Initial keys should be empty
                cache.getKeys().shouldBe(emptySet())

                // ReadOrCreate should create new document
                cache.readOrCreate("readOrCreateKeysKey1") { doc ->
                    doc.copy(name = "ReadOrCreate Keys Doc 1", balance = 100.0)
                }.getOrThrow()

                var keys = cache.getKeys()
                keys.size shouldBe 1
                keys shouldBe setOf("readOrCreateKeysKey1")

                // ReadOrCreate should read existing document (no key change)
                cache.readOrCreate("readOrCreateKeysKey1") { doc ->
                    doc.copy(name = "ReadOrCreate Keys Doc 1 Updated", balance = 150.0)
                }.getOrThrow()

                keys = cache.getKeys()
                keys.size shouldBe 1
                keys shouldBe setOf("readOrCreateKeysKey1")

                // Create another document
                cache.readOrCreate("readOrCreateKeysKey2") { doc ->
                    doc.copy(name = "ReadOrCreate Keys Doc 2", balance = 200.0)
                }.getOrThrow()

                keys = cache.getKeys()
                keys.size shouldBe 2
                keys shouldBe setOf("readOrCreateKeysKey1", "readOrCreateKeysKey2")
            }

            it("should verify keys with random key creation") {
                val cache = getCache()

                // Initial keys should be empty
                cache.getKeys().shouldBe(emptySet())

                // Create document with random key
                val randomDoc1 = cache.createRandom { doc ->
                    doc.copy(name = "Random Keys Doc 1", balance = 100.0)
                }.getOrThrow()

                var keys = cache.getKeys()
                keys.size shouldBe 1
                keys shouldBe setOf(randomDoc1.key)

                // Create another document with random key
                val randomDoc2 = cache.createRandom { doc ->
                    doc.copy(name = "Random Keys Doc 2", balance = 200.0)
                }.getOrThrow()

                keys = cache.getKeys()
                keys.size shouldBe 2
                keys shouldBe setOf(randomDoc1.key, randomDoc2.key)
            }

            it("should verify isCached behavior after operations") {
                val cache = getCache()

                // Check non-existent key
                cache.isCached("nonExistentKey").shouldBe(false)

                // Create a document
                cache.create("isCachedTestKey") { doc ->
                    doc.copy(name = "Is Cached Test Doc", balance = 100.0)
                }.getOrThrow()

                // Check existing key
                cache.isCached("isCachedTestKey").shouldBe(true)

                // Update document
                cache.update("isCachedTestKey") { doc ->
                    doc.copy(name = "Updated Is Cached Test Doc", balance = 200.0)
                }.getOrThrow()

                // Should still be cached
                cache.isCached("isCachedTestKey").shouldBe(true)

                // Delete document
                cache.delete("isCachedTestKey").getOrThrow()

                // Should no longer be cached
                cache.isCached("isCachedTestKey").shouldBe(false)
            }

            it("should verify keys after cache clear operations") {
                val cache = getCache()

                // Create multiple documents
                cache.create("clearKeysKey1") { doc ->
                    doc.copy(name = "Clear Keys Doc 1", balance = 100.0)
                }.getOrThrow()

                cache.create("clearKeysKey2") { doc ->
                    doc.copy(name = "Clear Keys Doc 2", balance = 200.0)
                }.getOrThrow()

                cache.create("clearKeysKey3") { doc ->
                    doc.copy(name = "Clear Keys Doc 3", balance = 300.0)
                }.getOrThrow()

                // Keys should be 3
                var keys = cache.getKeys()
                keys.size shouldBe 3
                keys shouldBe setOf("clearKeysKey1", "clearKeysKey2", "clearKeysKey3")

                // Delete all documents
                cache.delete("clearKeysKey1").getOrThrow()
                cache.delete("clearKeysKey2").getOrThrow()
                cache.delete("clearKeysKey3").getOrThrow()

                // Keys should be empty
                keys = cache.getKeys()
                keys.shouldBe(emptySet())
            }
        }
    }
}
