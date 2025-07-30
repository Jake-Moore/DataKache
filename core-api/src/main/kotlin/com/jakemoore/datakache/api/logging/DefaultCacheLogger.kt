package com.jakemoore.datakache.api.logging

open class DefaultCacheLogger(protected val nickname: String) : DefaultLoggerService() {
    override val loggerName: String
        get() = "C: $nickname"
}
