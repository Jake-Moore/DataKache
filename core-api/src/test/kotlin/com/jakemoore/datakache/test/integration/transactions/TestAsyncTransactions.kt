package com.jakemoore.datakache.test.integration.transactions

import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import com.jakemoore.datakache.util.doc.TestGenericDoc
import io.kotest.core.spec.Order
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import java.util.concurrent.atomic.AtomicInteger

@Order(1)
@Suppress("unused")
class TestAsyncTransactions : AbstractDataKacheTest() {

    init {
        describe("Async Transactions") {
            it("should handle concurrent transactions atomically") {
                val doc = cache.createRandom {
                    it.copy(
                        balance = 0.0,
                        list = listOf("Initial entry")
                    )
                }.getOrThrow()

                // Queue multiple transactions to modify the same document concurrently
                val msStart = System.currentTimeMillis()
                val atomicCount = AtomicInteger(0)
                val deferred = (1..THREAD_COUNT).map { i ->
                    async {
                        // coroutine scheduler will break FIFO unless we deliberately delay each enough
                        //  to ensure they are processed in the order we want
                        delay(i * DELAY_MS_PER_JOB)
                        val r = cache.update(doc.key) {
                            it.copy(
                                list = it.list + "Thread $i started"
                            )
                        }
                        val finished = atomicCount.addAndGet(1)
                        val elapsed = System.currentTimeMillis() - msStart
                        System.err.println("Async Transaction $i finished. ($finished/$THREAD_COUNT) in ${elapsed}ms")
                        return@async r
                    }
                }
                val results = deferred.awaitAll()
                val msEnd = System.currentTimeMillis()

                // Verify that all transactions were successful
                results.all { it is Success<TestGenericDoc> } shouldBe true

                // Verify the final state of the document
                val finalResult = cache.read(doc.key)
                finalResult.shouldBeInstanceOf<Success<TestGenericDoc>>()
                val finalDoc = finalResult.value
                finalDoc.list.size shouldBe THREAD_COUNT + 1 // +1 for the initial entry

                // Uncomment for debugging
                // val jsonString = json.encodeToString(TestGenericDoc.serializer(), finalDoc)
                // System.err.println("Final Document JSON:")
                // System.err.println(jsonString)

                // Check that all entries are present
                val fullList = mutableListOf("Initial entry")
                fullList.addAll((1..THREAD_COUNT).map { "Thread $it started" })
                finalDoc.list.size shouldBe fullList.size
                finalDoc.list.toSet() shouldBe fullList.toSet()

                // Check if they are in the expected order
                finalDoc.list shouldBe fullList
            }
        }
    }

    companion object {
        private const val THREAD_COUNT: Int = 50
        private const val DELAY_MS_PER_JOB: Long = 30L
    }
}
