package com.jakemoore.datakache.test.integration.transactions

import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import io.kotest.core.spec.Order
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicInteger

@Order(2)
@Suppress("unused")
class TestAsyncShutdown : AbstractDataKacheTest() {

    private val atomicCount = AtomicInteger(0)

    init {
        describe("Async Transactions") {
            it("should handle concurrent transactions atomically") {
                val doc = cache.createRandom {
                    it.copy(
                        balance = 0.0,
                        list = mutableListOf("Initial entry")
                    )
                }.getOrThrow()

                // Queue multiple transactions to modify the same document concurrently
                val msStart = System.currentTimeMillis()
                (1..THREAD_COUNT).map { i ->
                    launch {
                        delay(i * 20L) // add delay in order to have coroutines processed in order
                        cache.update(doc.key) {
                            it.copy(
                                list = it.list + "Thread $i started"
                            )
                        }
                        val finished = atomicCount.addAndGet(1)
                        val elapsed = System.currentTimeMillis() - msStart
                        System.err.println("Async Transaction $i finished. ($finished/$THREAD_COUNT) in ${elapsed}ms")
                    }
                }
                // Child coroutines are tied to the test coroutine scope; Kotest **should** wait for them to complete.
            }
        }

        afterSpec {
            System.err.println("Total Completed Transactions: ${atomicCount.get()} / $THREAD_COUNT")
            atomicCount.get() shouldBe THREAD_COUNT
        }
    }

    companion object {
        private const val THREAD_COUNT: Int = 50
    }
}
