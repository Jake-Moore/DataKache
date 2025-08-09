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
    private val tempLogFolder = createTempDirectory(prefix = "datakache-test-logs-")
        .toFile()
        .apply { deleteOnExit() }
    private val contextConfig = testContainer.dataKacheConfig

    init {
        // Ensure logs folder is fully cleaned up (recursively) at JVM exit
        Runtime.getRuntime().addShutdownHook(Thread { tempLogFolder.deleteRecursively() })
    }

    override val logger: LoggerService = object : LoggerService {
        override val loggerName: String
            get() = "TestDataKacheLogger"
        override val permitsDebugStatements: Boolean
            get() = true

        override fun logToConsole(
            msg: String,
            level: LoggerService.LogLevel
        ) {
            when (level) {
                LoggerService.LogLevel.WARNING,
                LoggerService.LogLevel.SEVERE -> System.err.println("[$loggerName] [$level] $msg")
                else -> println("[$loggerName] [$level] $msg")
            }
        }
    }

    override val config: DataKacheConfig
        get() = contextConfig

    override val logFolder: File
        get() = tempLogFolder
}
