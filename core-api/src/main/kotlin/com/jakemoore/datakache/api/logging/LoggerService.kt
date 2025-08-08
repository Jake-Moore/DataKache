package com.jakemoore.datakache.api.logging

// TODO we still need a better color solution other than sending minecraft ampersand codes like &a in logs
@Suppress("unused", "MemberVisibilityCanBePrivate")
interface LoggerService {
    enum class LogLevel {
        DEBUG,
        INFO,
        WARNING,
        SEVERE,
    }

    // Abstraction
    val loggerName: String
    val permitsDebugStatements: Boolean

    fun logToConsole(msg: String, level: LogLevel)

    // Public Methods
    fun info(msg: String) {
        logToConsole(msg, LogLevel.INFO)
    }

    fun info(throwable: Throwable) {
        logToConsole(throwable.message ?: "Unknown Throwable Error", LogLevel.INFO)
        throwable.printStackTrace()
    }

    fun info(throwable: Throwable, msg: String) {
        logToConsole(msg + " - " + throwable.message, LogLevel.INFO)
        throwable.printStackTrace()
    }

    fun debug(msg: String) {
        if (!permitsDebugStatements) {
            return
        }
        logToConsole(msg, LogLevel.DEBUG)
    }

    fun warn(msg: String) {
        logToConsole(msg, LogLevel.WARNING)
    }

    fun warn(throwable: Throwable) {
        logToConsole(throwable.message ?: "Unknown Throwable Error", LogLevel.WARNING)
        throwable.printStackTrace()
    }

    fun warn(throwable: Throwable, msg: String) {
        logToConsole(msg + " - " + throwable.message, LogLevel.WARNING)
        throwable.printStackTrace()
    }

    fun warning(msg: String) {
        this.warn(msg)
    }

    fun warning(throwable: Throwable) {
        this.warn(throwable)
    }

    fun warning(throwable: Throwable, msg: String) {
        this.warn(throwable, msg)
    }

    fun severe(msg: String) {
        logToConsole(msg, LogLevel.SEVERE)
    }

    fun severe(throwable: Throwable) {
        logToConsole(throwable.message ?: "Unknown Throwable Error", LogLevel.SEVERE)
        throwable.printStackTrace()
    }

    fun severe(throwable: Throwable, msg: String) {
        logToConsole(msg + " - " + throwable.message, LogLevel.SEVERE)
        throwable.printStackTrace()
    }

    fun error(msg: String) {
        this.severe(msg)
    }

    fun error(throwable: Throwable) {
        this.severe(throwable)
    }

    fun error(throwable: Throwable, msg: String) {
        this.severe(throwable, msg)
    }
}
