@file:Suppress("unused", "UnusedReceiverParameter")

package com.jakemoore.datakache.core.connections

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlin.coroutines.CoroutineContext

internal interface DatabaseScope : CoroutineScope {
    override val coroutineContext: CoroutineContext
        get() = GlobalDatabaseScope.coroutineContext
}

/**
 * Internal [CoroutineScope] implementation for [DatabaseScope].
 */
@Suppress("MemberVisibilityCanBePrivate")
internal object GlobalDatabaseScope : CoroutineScope {
    // SupervisorJob ensures that a child coroutine failure won't cancel the parent job or other child coroutines.
    private var supervisorJob = SupervisorJob()

    // Create the Context (from IO Dispatcher, with SupervisorJob and CoroutineExceptionHandler)
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + supervisorJob

    internal fun restart() {
        // Cancel any existing job if still active
        if (supervisorJob.isActive) {
            supervisorJob.cancel()
        }
        // Create a fresh job and handler
        supervisorJob = SupervisorJob()
    }

    // Terminate all scopes and coroutines
    internal fun shutdown() {
        supervisorJob.cancel()
    }
}
