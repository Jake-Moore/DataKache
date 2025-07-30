package com.jakemoore.datakache.util

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.logging.LoggerService
import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.io.PrintWriter
import java.util.UUID

/**
 * Logs full stack traces to a file while outputting concise warnings to the console.
 *
 * This prevents console spam but still captures complete trace details
 * for developer debugging in the log file.
 */
@Suppress("unused", "MemberVisibilityCanBePrivate")
object DataKacheFileLogger {
    private val DEBUG = LoggerService.LogLevel.DEBUG
    private val INFO = LoggerService.LogLevel.INFO
    private val WARN = LoggerService.LogLevel.WARNING
    private val SEVERE = LoggerService.LogLevel.SEVERE

    // ------------------------------------------------------------ //
    //                        Logging Methods                       //
    // ------------------------------------------------------------ //

    fun debug(msg: String): File? = logToFile(msg, DEBUG, randomFile)
    fun debug(cache: DocCache<*, *>, msg: String): File? = logToFile(msg, DEBUG, getFileByCache(cache))
    fun debug(cache: DocCache<*, *>, msg: String, trace: Throwable): File? = debug(msg, trace, getFileByCache(cache))
    fun debug(msg: String, trace: Throwable): File? = debug(msg, trace, randomFile)
    fun debug(msg: String, trace: Throwable, file: File): File? = logToFile(msg, DEBUG, file, trace)

    fun info(msg: String): File? = logToFile(msg, INFO, randomFile)
    fun info(cache: DocCache<*, *>, msg: String): File? = logToFile(msg, INFO, getFileByCache(cache))
    fun info(cache: DocCache<*, *>, msg: String, trace: Throwable): File? = info(msg, trace, getFileByCache(cache))
    fun info(msg: String, trace: Throwable): File? = info(msg, trace, randomFile)
    fun info(msg: String, trace: Throwable, file: File): File? = logToFile(msg, INFO, file, trace)

    fun warn(msg: String): File? = logToFile(msg, WARN, randomFile)
    fun warn(cache: DocCache<*, *>, msg: String): File? = logToFile(msg, WARN, getFileByCache(cache))
    fun warn(cache: DocCache<*, *>, msg: String, trace: Throwable): File? = warn(msg, trace, getFileByCache(cache))
    fun warn(msg: String, trace: Throwable): File? = warn(msg, trace, randomFile)
    fun warn(msg: String, trace: Throwable, file: File): File? = logToFile(msg, WARN, file, trace)

    fun severe(msg: String): File? = logToFile(msg, SEVERE, randomFile)
    fun severe(cache: DocCache<*, *>, msg: String): File? = logToFile(msg, SEVERE, getFileByCache(cache))
    fun severe(cache: DocCache<*, *>, msg: String, trace: Throwable): File? = severe(msg, trace, getFileByCache(cache))
    fun severe(msg: String, trace: Throwable): File? = severe(msg, trace, randomFile)
    fun severe(msg: String, trace: Throwable, file: File): File? = logToFile(msg, SEVERE, file, trace)

    // ------------------------------------------------------------ //
    //                         Helper Methods                       //
    // ------------------------------------------------------------ //

    private fun appendToFile(throwable: Throwable, file: File): Boolean {
        return appendToFile(file) { writer ->
            throwable.printStackTrace(writer)
        }
    }

    private fun appendToFile(lines: List<String>, file: File): Boolean {
        return appendToFile(file) { writer ->
            lines.forEach(writer::println)
        }
    }

    private fun appendToFile(file: File, writerAction: (PrintWriter) -> Unit): Boolean {
        // ensure parent dirs exist
        file.parentFile
            ?.takeIf { !it.exists() }
            ?.mkdirs()

        return try {
            FileWriter(file, true).use { fw ->
                PrintWriter(fw).use { pw ->
                    writerAction(pw)
                    true
                }
            }
        } catch (e: IOException) {
            DataKache.logger.severe("Failed to write stack trace to file (" + file.absoluteFile + "): " + e.message)
            false
        }
    }

    private fun createStackTrace(msg: String): Throwable {
        try {
            throw kotlin.Exception(msg)
        } catch (t: Throwable) {
            return t
        }
    }

    // Logs a message to a file
    private fun logToFile(msg: String, level: LoggerService.LogLevel, file: File): File? {
        if (appendToFile(createStackTrace(msg), file)) {
            DataKache.logger.logToConsole(msg + " (Logged to " + file.absolutePath + ")", level)
            return file
        }
        return null
    }

    // Logs a message (and a stack trace) to a file
    private fun logToFile(msg: String, level: LoggerService.LogLevel, file: File, trace: Throwable): File? {
        val outputFile = logToFile(msg, level, file) ?: return null

        // Add some empty lines for separation
        if (!appendToFile(listOf("", "", "Extra Trace", ""), outputFile)) {
            return null
        }

        // Save the original trace after
        if (!appendToFile(trace, outputFile)) {
            return null
        }
        return outputFile
    }

    private fun getFileByCache(docCache: DocCache<*, *>): File {
        // Print the message + a stack trace to a file
        requireNotNull(DataKache.context).logFolder
        val now = System.currentTimeMillis()

        val fileName = docCache.registration.client.name + "_" + docCache.cacheName + "_" + now + ".log"
        val logsFolder = File(requireNotNull(DataKache.context).logFolder, "logs")
        val datakacheFolder = File(logsFolder, "datakache")
        return File(datakacheFolder, fileName)
    }

    private val randomFile: File
        get() {
            val fileName = "log_" + System.currentTimeMillis() + "_" + UUID.randomUUID() + ".log"

            val logsFolder = File(requireNotNull(DataKache.context).logFolder, "logs")
            val datakacheFolder = File(logsFolder, "datakache")
            return File(datakacheFolder, fileName)
        }

    internal val randomWriteExceptionFile: File
        get() {
            val fileName = "log_" + System.currentTimeMillis() + "_" + UUID.randomUUID() + ".log"

            val logsFolder = File(requireNotNull(DataKache.context).logFolder, "logs")
            val datakacheFolder = File(logsFolder, "datakache")
            return File(datakacheFolder, fileName)
        }
}
