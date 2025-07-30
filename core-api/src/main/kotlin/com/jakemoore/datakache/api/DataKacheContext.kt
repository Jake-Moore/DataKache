package com.jakemoore.datakache.api

import com.jakemoore.datakache.api.logging.LoggerService
import java.io.File

/**
 * Supplies all the "environment" information needed for DataKache to start up.
 */
interface DataKacheContext {
    val logger: LoggerService
    val config: DataKacheConfig

    /**
     * The root folder for DataKache to place log files. This should be a writable directory.
     */
    val logFolder: File
}
