package com.jakemoore.datakache.api

import com.jakemoore.datakache.api.logging.LoggerService

/**
 * Supplies all the "environment" information needed for DataKache to start up.
 */
interface DataKacheContext {
    val logger: LoggerService
    val config: DataKacheConfig
}
