package com.jakemoore.datakache.api

import com.jakemoore.datakache.api.mode.StorageMode

/**
 * Configuration for DataKache.
 */
class DataKacheConfig(
    val databasePrefix: String = "global",
    val debug: Boolean = false,
    val storageMode: StorageMode,

    // MongoDB Connection Details
    val mongoURI: String = "mongodb://localhost:27017",
)
