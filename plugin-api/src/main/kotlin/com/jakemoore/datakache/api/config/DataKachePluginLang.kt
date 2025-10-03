package com.jakemoore.datakache.api.config

import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

data class DataKachePluginLang(
    val joinDeniedDatabaseNotReady: String =
        "&cServer is starting up! Please try again in a few seconds.",
    val preloadPlayerDocTimeout: Duration =
        5_000.milliseconds,
    val joinDeniedPlayerDocTimeout: String =
        "&cOops! Profile failed to load in time. Please try again in a few seconds.",
    val joinDeniedPlayerDocException: String =
        "&cOops! Something unexpected went wrong. Please try again in a few seconds.",
    val joinDeniedEarlyJoin: String =
        "&cOops! You joined too fast! Please try again in a few seconds.",
)
