package com.jakemoore.datakache.util.core

import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.DataKacheContext
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.util.core.container.DataKacheTestContainer
import java.io.File
import kotlin.io.path.createTempDirectory

/**
 * Test implementation of DataKacheContext that uses a TestContextFactory
 * to provide database-specific configuration.
 *
 * This allows for database-agnostic test setup while maintaining
 * database-specific configuration logic.
 */
class TestDataKacheContext(
    testContainer: DataKacheTestContainer,
) : DataKacheContext {
    private val tempLogFolder = createTempDirectory(prefix = "datakache-test-logs-").toFile()
    private val contextConfig = testContainer.getKacheConfig()

    override val logger: LoggerService = object : LoggerService {
        override val loggerName: String
            get() = "TestDataKacheLogger"
        override val permitsDebugStatements: Boolean
            get() = true

        override fun logToConsole(
            msg: String,
            level: LoggerService.LogLevel
        ) {
            println("[$level] $msg")
        }
    }

    override val config: DataKacheConfig
        get() = contextConfig

    override val logFolder: File
        get() = tempLogFolder
}