@file:Suppress("unused")

package com.jakemoore.datakache.api.logging

/**
 * Default (and most simple) implementation of the [LoggerService].
 */
open class DefaultLoggerService : LoggerService {
    override val loggerName: String
        get() = "DataKache"

    override val permitsDebugStatements: Boolean
        get() = true

    override fun logToConsole(msg: String, level: LoggerService.LogLevel) {
        // Simple console logging
        if (level == LoggerService.LogLevel.SEVERE) {
            System.err.println("[$level] $msg")
        } else {
            println("[$level] $msg")
        }
    }
}
