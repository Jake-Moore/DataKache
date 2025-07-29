package com.jakemoore.datakache.core

@Suppress("unused")
internal interface Service {
    suspend fun start(): Boolean

    suspend fun shutdown(): Boolean

    val running: Boolean
}
