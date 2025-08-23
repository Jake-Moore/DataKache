package com.jakemoore.datakache.util

import kotlinx.coroutines.delay
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

suspend fun <T> eventually(
    timeout: Duration = 10.seconds,
    interval: Duration = 200.milliseconds,
    block: suspend () -> T
): T {
    val deadline = System.nanoTime() + timeout.inWholeNanoseconds
    var lastError: Throwable? = null

    while (System.nanoTime() < deadline) {
        try {
            return block()
        } catch (t: Throwable) {
            lastError = t
        }
        delay(interval)
    }
    throw AssertionError("Condition not met within $timeout", lastError)
}
