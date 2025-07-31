@file:Suppress("UnusedReceiverParameter", "unused")

package com.jakemoore.datakache.api.coroutines

import com.jakemoore.datakache.DataKachePlugin
import org.bukkit.Bukkit
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

// -------------------------------------------------------------------------------- //
//                      Extension functions for DataKacheScope                     //
// -------------------------------------------------------------------------------- //
/**
 * Runs the given [runnable] synchronously on the main server thread.
 *
 * DOES NOT WAIT for the runnable to complete, it just schedules it.
 */
fun DataKacheScope.runSync(runnable: Runnable) {
    val plugin = requireNotNull(DataKachePlugin.getController())
    Bukkit.getScheduler().runTask(plugin, runnable)
}

/**
 * Runs the given [block] synchronously on the main server thread.
 *
 * WAITS for the block to complete before returning.
 */
suspend fun DataKacheScope.runSyncBlocking(block: () -> Unit) {
    return suspendCoroutine { continuation ->
        val plugin = requireNotNull(DataKachePlugin.getController())
        Bukkit.getScheduler().runTask(plugin) {
            try {
                block()
                continuation.resume(Unit)
            } catch (e: Exception) {
                continuation.resumeWithException(e)
            }
        }
    }
}

/**
 * Runs the given [block] synchronously on the main server thread and returns a result of type [T].
 *
 * WAITS for the block to complete before returning result [T].
 */
suspend fun <T> DataKacheScope.runSyncFetching(block: () -> T): T {
    return suspendCoroutine { continuation ->
        val plugin = requireNotNull(DataKachePlugin.getController())
        Bukkit.getScheduler().runTask(plugin) {
            try {
                val result: T = block()
                continuation.resume(result)
            } catch (e: Exception) {
                continuation.resumeWithException(e)
            }
        }
    }
}
