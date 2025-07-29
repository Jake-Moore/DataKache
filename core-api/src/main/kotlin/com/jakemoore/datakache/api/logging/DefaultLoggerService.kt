@file:Suppress("unused")

package com.jakemoore.datakache.api.logging

/**
 * Default (and most simple) implementation of the [LoggerService].
 */
class DefaultLoggerService : LoggerService {
    override val loggerName: String
        get() = "DataKache"

    override val permitsDebugStatements: Boolean
        get() = false

    override fun logToConsole(msg: String, level: LoggerService.LogLevel) {
        // Simple console logging
        println("[$level] $msg")
    }
}
