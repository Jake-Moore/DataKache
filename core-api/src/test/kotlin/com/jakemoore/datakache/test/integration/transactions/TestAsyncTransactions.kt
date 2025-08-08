package com.jakemoore.datakache.test.integration.transactions

import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.serialization.json.Json
import java.util.concurrent.atomic.AtomicInteger

@Suppress("unused")
class TestAsyncTransactions : AbstractDataKacheTest() {

    private val json = Json {
        encodeDefaults = true // Encodes default data class property values (instead of omitting them)
        explicitNulls = true // Encodes null values (instead of omitting them)
        prettyPrint = true
    }

    init {
        describe("Async Transactions") {
            it("should handle concurrent transactions atomically") {
                val cache = getCache()
                val doc = cache.createRandom {
                    it.copy(
                        balance = 0.0,
                        list = mutableListOf("Initial entry")
                    )
                }.getOrThrow()
                val version = doc.version

                // Queue multiple transactions to modify the same document concurrently
                val msStart = System.currentTimeMillis()
                val atomicCount = AtomicInteger(0)
                val deferred = (1..THREAD_COUNT).map { i ->
                    async {
                        // delay(i * 20L) // add delay in order to have coroutines processed in order
                        val r = cache.update(doc.key) {
                            it.copy(
                                list = it.list + "Thread $i started"
                            )
                        }
                        val finished = atomicCount.addAndGet(1)
                        val elapsed = System.currentTimeMillis() - msStart
                        System.err.println("Async Transaction $i finished. ($finished/$THREAD_COUNT) in ${elapsed}ms)")
                        return@async r
                    }
                }
                val results = deferred.awaitAll()
                val msEnd = System.currentTimeMillis()

                // Verify that all transactions were successful
                val successCount = results.count { it is Success<TestGenericDoc> }
                successCount.shouldBe(THREAD_COUNT)

                // Verify the final state of the document
                val finalResult = cache.read(doc.key)
                finalResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                val finalDoc = finalResult.value
                finalDoc.list.size shouldBe THREAD_COUNT + 1 // +1 for the initial entry

                val jsonString = json.encodeToString(TestGenericDoc.serializer(), finalDoc)
                System.err.println("Final Document JSON:")
                System.err.println(jsonString)

                // Check that all entries are present
                val fullList = mutableListOf("Initial entry")
                fullList.addAll((1..THREAD_COUNT).map { "Thread $it started" })
                finalDoc.list.size shouldBe fullList.size
                finalDoc.list.toSet() shouldBe fullList.toSet()

                // Check if they are in the expected order
                if (fullList == finalDoc.list) {
                    System.err.println("Entries are in the expected order.")
                } else {
                    System.err.println("! Entries are NOT in the expected order.")
                }

                System.err.println(
                    "All async transactions completed. Total time: ${msEnd - msStart}ms"
                )
            }
        }
    }

    companion object {
        private const val THREAD_COUNT: Int = 50
    }
}
