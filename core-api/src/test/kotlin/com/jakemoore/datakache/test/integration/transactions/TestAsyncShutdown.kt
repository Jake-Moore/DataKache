package com.jakemoore.datakache.test.integration.transactions

import com.jakemoore.datakache.api.coroutines.GlobalDataKacheScope
import com.jakemoore.datakache.util.core.AbstractDataKacheTest
import io.kotest.core.spec.Order
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.TimeSource

@Order(2)
@Suppress("unused")
class TestAsyncShutdown : AbstractDataKacheTest() {

    private val atomicCount = AtomicInteger(0)

    init {
        describe("Async Shutdown") {
            it("should await all DataKacheScope coroutines before shutdown") {
                val docCache = this@TestAsyncShutdown.cache
                val doc = docCache.createRandom {
                    it.copy(
                        balance = 0.0,
                        list = mutableListOf("Initial entry")
                    )
                }.getOrThrow()

                // Queue multiple transactions to modify the same document concurrently
                val startMark = TimeSource.Monotonic.markNow()
                (1..THREAD_COUNT).map { i ->
                    // attach coroutine to GlobalDataKacheScope
                    GlobalDataKacheScope.launch {
                        delay(i * DELAY_MS_PER_JOB) // add delay in order to have coroutines processed in order
                        val finished = atomicCount.addAndGet(1)
                        val elapsedMillis = startMark.elapsedNow().inWholeMilliseconds
                        System.err.println(
                            "Scope Transaction $i finished. ($finished/$THREAD_COUNT) in ${elapsedMillis}ms"
                        )
                    }
                }

                // Child coroutines are tied DataKacheScope; DataKache **should** wait for them to complete.
                // i.e. kotest afterEach should get blocked waiting for them to finish.
                delay(100) // wait a bit to allow coroutines to start
            }
        }

        afterSpec {
            System.err.println("Total Completed Transactions: ${atomicCount.get()} / $THREAD_COUNT")
            atomicCount.get() shouldBe THREAD_COUNT
        }
    }

    companion object {
        private const val THREAD_COUNT: Int = 50
        private const val DELAY_MS_PER_JOB: Long = 50L
    }
}
