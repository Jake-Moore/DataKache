package com.jakemoore.datakache.api.config

import org.bukkit.configuration.file.FileConfiguration
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

data class DataKachePluginLang(
    val joinDeniedDatabaseNotReady: String =
        "&cServer is starting up! Please try again in a few seconds.",
    val preloadPlayerDocTimeoutMS: Duration =
        5_000.milliseconds,
    val joinDeniedPlayerDocTimeout: String =
        "&cOops! Profile failed to load in time. Please try again in a few seconds.",
    val joinDeniedPlayerDocException: String =
        "&cOops! Something unexpected went wrong. Please try again in a few seconds.",
    val joinDeniedEarlyJoin: String =
        "&cOops! You joined too fast! Please try again in a few seconds.",
) {
    companion object {
        fun loadFromFileConfiguration(config: FileConfiguration): DataKachePluginLang {
            val defaults = DataKachePluginLang()

            return DataKachePluginLang(
                joinDeniedDatabaseNotReady = config.getString("language.joinDenied.databaseNotReady")
                    ?: defaults.joinDeniedDatabaseNotReady,
                preloadPlayerDocTimeoutMS = config.getInt("joinOptions.preloadPlayerDocTimeoutMS", -1)
                    .takeIf { it > 0 }?.milliseconds ?: defaults.preloadPlayerDocTimeoutMS,
                joinDeniedPlayerDocTimeout = config.getString("language.joinDenied.playerDocTimeout")
                    ?: defaults.joinDeniedPlayerDocTimeout,
                joinDeniedPlayerDocException = config.getString("language.joinDenied.playerDocException")
                    ?: defaults.joinDeniedPlayerDocException,
                joinDeniedEarlyJoin = config.getString("language.joinDenied.earlyJoin")
                    ?: defaults.joinDeniedEarlyJoin,
            )
        }
    }
}
