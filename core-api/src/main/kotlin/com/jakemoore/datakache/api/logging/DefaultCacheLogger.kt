package com.jakemoore.datakache.api.logging

open class DefaultCacheLogger(protected val cacheName: String) : DefaultLoggerService() {
    override val loggerName: String
        get() = "C: $cacheName"
}
