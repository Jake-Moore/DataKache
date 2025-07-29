@file:Suppress("unused")

package com.jakemoore.datakache

import com.jakemoore.datakache.api.DataKacheAPI
import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.DataKacheContext
import com.jakemoore.datakache.api.coroutines.GlobalDataKacheScope
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.mode.StorageMode
import kotlinx.coroutines.runBlocking

object DataKache {
    private var enabled = false
    private var context: DataKacheContext? = null

    // Internal Properties
    internal var onEnableTime: Long = 0

    /**
     * This method will always successfully enable DataKache.
     * @param context The [DataKacheContext] to use for initialization.
     * @return true IFF a plugin source was NEEDED and used for registration. (false if already enabled)
     */
    suspend fun onEnable(context: DataKacheContext): Boolean {
        if (enabled) {
            return false
        }
        DataKache.context = context
        enabled = true

        // Application Modes
        info("Running in '$storageMode' storage mode.")

        // Enable Services
        storageMode.enableServices()

        onEnableTime = System.currentTimeMillis()
        return true
    }

    /**
     * @return true IFF this call triggered the disable sequence, false it already disabled
     */
    suspend fun onDisable(): Boolean {
        if (!enabled) {
            return false
        }

        // Wait for Coroutines
        logger.info("&aWaiting for all coroutines to finish...")
        runBlocking {
            if (!GlobalDataKacheScope.awaitAllChildrenCompletion(logger)) {
                logger.severe("&cFailed to wait for all coroutines to finish!")
                GlobalDataKacheScope.logActiveCoroutines()
            }
            GlobalDataKacheScope.cancelAll()
        }
        logger.info("&aAll coroutines finished!")

        // Shutdown Dangling Collections (and warn authors)
        if (DataKacheAPI.registrations.isNotEmpty()) {
            // This should be safe (although not ideal)
            //   since any plugin depending on DataKache should be disabled before this
            DataKacheAPI.registrations.forEach {
                logger.warning(
                    "DataKacheRegistration for ${it.databaseName} " +
                        "(backed by plugin '${it.client.name}') was not shutdown before plugin disable."
                )
                logger.warning(
                    "Manually shutting it down! " +
                        "(PLEASE CONTACT AUTHORS OF ${it.client.name} TO FIX THIS)"
                )
                runBlocking { it.shutdown() }
            }
            DataKacheAPI.registrations.clear()
        }

        // Shutdown Services
        storageMode.disableServices()

        // Reset State
        enabled = false
        return true
    }

    // ----------------------------- //
    //         DataKacheConfig       //
    // ----------------------------- //
    val config: DataKacheConfig
        get() = requireNotNull(context).config

    val logger: LoggerService
        get() = requireNotNull(context).logger

    val storageMode: StorageMode
        get() = config.storageMode

    // Server Identification
    val databasePrefix: String
        get() = config.databasePrefix

    // ----------------------------- //
    //       DataKache Logging       //
    // ----------------------------- //
    internal fun info(msg: String) {
        val logger: LoggerService? = context?.logger
        if (logger == null) {
            println("[INFO] $msg")
        } else {
            logger.info(msg)
        }
    }

    internal fun warning(msg: String) {
        val logger: LoggerService? = context?.logger
        if (logger == null) {
            println("[WARNING] $msg")
        } else {
            logger.warning(msg)
        }
    }

    internal fun error(msg: String) {
        val logger: LoggerService? = context?.logger
        if (logger == null) {
            println("[ERROR] $msg")
        } else {
            logger.severe(msg)
        }
    }

    /**
     * @return If DataKache debug logging is enabled
     */
    fun isDebug(): Boolean {
        return config.debug
    }
}
