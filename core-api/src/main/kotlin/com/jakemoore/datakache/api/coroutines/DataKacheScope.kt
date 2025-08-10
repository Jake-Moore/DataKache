@file:Suppress("unused", "UnusedReceiverParameter")

package com.jakemoore.datakache.api.coroutines

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.coroutines.DataKacheScope.Companion.EXCEPTION_CONSUMERS
import com.jakemoore.datakache.api.coroutines.exception.ExceptionConsumer
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.core.connections.GlobalDatabaseScope
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlin.coroutines.CoroutineContext

/**
 * Primary [CoroutineScope] for DataKache operations. Should be used by all DataKache-related coroutines.
 *
 * This includes internal operations AND external plugins when using coroutines from DataKache.
 */
interface DataKacheScope : CoroutineScope {
    override val coroutineContext: CoroutineContext
        get() = GlobalDataKacheScope.coroutineContext

    companion object {
        val EXCEPTION_CONSUMERS: MutableList<ExceptionConsumer> = mutableListOf()

        fun runningCoroutinesCount(): Int {
            return GlobalDataKacheScope.coroutineContext[Job]?.children?.count() ?: 0
        }
    }
}

/**
 * Internal [CoroutineScope] implementation for [DataKacheScope].
 */
@Suppress("MemberVisibilityCanBePrivate")
internal object GlobalDataKacheScope : CoroutineScope {

    // SupervisorJob ensures that a child coroutine failure won't cancel the parent job or other child coroutines.
    private var supervisorJob = SupervisorJob()

    // CoroutineExceptionHandler to log errors
    private var exceptionHandler = CoroutineExceptionHandler { _, throwable ->
        if (EXCEPTION_CONSUMERS.isEmpty()) {
            DataKache.logger.severe("Unhandled coroutine error: $throwable")
            throwable.printStackTrace()
        } else {
            EXCEPTION_CONSUMERS.forEach { it.accept(throwable) }
        }
    }

    // Create the Context (from IO Dispatcher, with SupervisorJob and CoroutineExceptionHandler)
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + supervisorJob + exceptionHandler

    internal fun restart() {
        runCatching {
            GlobalDatabaseScope.restart()
        }

        // Cancel any existing job if still active
        if (supervisorJob.isActive) {
            supervisorJob.cancel()
        }
        // Create a fresh job and handler
        supervisorJob = SupervisorJob()
        exceptionHandler = CoroutineExceptionHandler { _, throwable ->
            if (EXCEPTION_CONSUMERS.isEmpty()) {
                DataKache.logger.severe("Unhandled coroutine error: $throwable")
                throwable.printStackTrace()
            } else {
                EXCEPTION_CONSUMERS.forEach { it.accept(throwable) }
            }
        }
    }

    // Terminate all scopes and coroutines
    internal fun shutdown() {
        runCatching {
            GlobalDatabaseScope.shutdown()
        }
        supervisorJob.cancel()
    }

    /**
     * @param maxWaitMS The maximum time to wait for all child coroutines to complete, in milliseconds.
     */
    internal fun awaitAllChildrenCompletion(logger: LoggerService, maxWaitMS: Long = 60_000L): Boolean {
        var totalWait = 0L
        val delayMS = 100L

        var loop = 0
        while (true) {
            val activeChildren = supervisorJob.children.toList()
            if (activeChildren.isEmpty()) {
                break
            }

            // Warn about non-terminated tasks every second
            if (delayMS * loop >= 1000L) {
                logger.warn("Waiting on Async Tasks: ${activeChildren.size} tasks running.")
                loop = 0
            }

            Thread.sleep(delayMS)
            totalWait += delayMS
            if (totalWait >= maxWaitMS) {
                return false
            }
            loop++
        }
        return true
    }

    internal fun logActiveCoroutines() {
        val job = coroutineContext[Job]
        val coroutines = job?.children?.toList() ?: emptyList()
        println("Active DataKacheScope Coroutines: ${coroutines.size}")
        coroutines.forEach { childJob ->
            println(
                "\tChild Job: " +
                    "isActive=${childJob.isActive}, " +
                    "isCompleted=${childJob.isCompleted}, " +
                    "isCancelled=${childJob.isCancelled}"
            )
        }
    }
}
