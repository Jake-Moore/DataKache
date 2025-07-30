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
    fun logToFile(msg: String, level: LoggerService.LogLevel, file: File): File? {
        if (appendToFile(createStackTrace(msg), file)) {
            DataKache.logger.logToConsole(msg + " (Logged to " + file.absolutePath + ")", level)
            return file
        }
        return null
    }

    fun warn(docCache: DocCache<*, *>, msg: String): File? {
        return logToFile(msg, LoggerService.LogLevel.WARNING, getFileByCache(docCache))
    }

    fun warn(docCache: DocCache<*, *>, msg: String, trace: Throwable): File? {
        val file = logToFile(
            msg,
            LoggerService.LogLevel.WARNING,
            getFileByCache(docCache)
        ) ?: return null

        // Add some empty lines for separation
        if (!appendToFile(listOf("", "", "Extra Trace", ""), file)) {
            return null
        }

        // Save the original trace after
        if (!appendToFile(trace, file)) {
            return null
        }
        return file
    }

    fun warn(msg: String): File? = logToFile(msg, LoggerService.LogLevel.WARNING, randomFile)

    fun warn(msg: String, trace: Throwable): File? = warn(msg, trace, randomFile)

    fun warn(msg: String, trace: Throwable, file: File): File? {
        return log(msg, trace, file, LoggerService.LogLevel.WARNING)
    }

    fun log(msg: String, trace: Throwable, file: File, level: LoggerService.LogLevel): File? {
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

    fun createStackTrace(msg: String): Throwable {
        try {
            throw kotlin.Exception(msg)
        } catch (t: Throwable) {
            return t
        }
    }

    fun appendToFile(throwable: Throwable, file: File): Boolean {
        val parent = file.parentFile
        if (parent != null && !parent.exists()) {
            parent.mkdirs()
        }

        try {
            FileWriter(file, true).use { fileWriter ->
                PrintWriter(fileWriter).use { printWriter ->
                    // Write the stack trace to the file
                    throwable.printStackTrace(printWriter)
                    return true
                }
            }
        } catch (e: IOException) {
            DataKache.logger.severe("Failed to write stack trace to file (" + file.absoluteFile + "): " + e.message)
            return false
        }
    }

    fun appendToFile(lines: List<String>, file: File): Boolean {
        val parent = file.parentFile
        if (parent != null && !parent.exists()) {
            parent.mkdirs()
        }

        try {
            FileWriter(file, true).use { fileWriter ->
                PrintWriter(fileWriter).use { printWriter ->
                    // Write the stack trace to the file
                    lines.forEach { x: String? -> printWriter.println(x) }
                    return true
                }
            }
        } catch (e: IOException) {
            DataKache.logger.severe("Failed to write stack trace to file (" + file.absoluteFile + "): " + e.message)
            return false
        }
    }

    private fun getFileByCache(docCache: DocCache<*, *>): File {
        // Print the message + a stack trace to a file
        requireNotNull(DataKache.context).logFolder
        val now = System.currentTimeMillis()

        val fileName = docCache.registration.client.name + "_" + docCache.nickname + "_" + now + ".log"
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
