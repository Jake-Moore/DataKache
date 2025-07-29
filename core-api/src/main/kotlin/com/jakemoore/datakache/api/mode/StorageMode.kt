package com.jakemoore.datakache.api.mode

// Abstraction layer for adding different storage modes in the future
enum class StorageMode {
    MONGODB;

    @Suppress("RedundantSuspendModifier")
    suspend fun enableServices() {
        // Enable Storage Service (just fetch storageService)
        when (this) {
            MONGODB -> {
                // TODO
            }
        }
    }

    @Suppress("RedundantSuspendModifier")
    suspend fun disableServices() {
        // TODO
    }
}
