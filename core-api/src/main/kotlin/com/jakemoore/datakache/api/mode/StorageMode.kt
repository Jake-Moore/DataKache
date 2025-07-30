package com.jakemoore.datakache.api.mode

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.core.connections.DatabaseService
import com.jakemoore.datakache.core.connections.mongo.MongoDatabaseService

// Abstraction layer for adding different storage modes in the future
enum class StorageMode {
    MONGODB;

    internal val databaseService: DatabaseService
        get() = when (this) {
            MONGODB -> getMongoService()
        }

    /**
     * @return if the services were successfully enabled
     */
    internal suspend fun enableServices(): Boolean {
        // Enable Storage Service (just fetch storageService)
        when (this) {
            MONGODB -> {
                val storage = MongoDatabaseService()
                if (!storage.start()) {
                    DataKache.logger.severe("Failed to start MongoDatabaseService, shutting down...")
                    return false
                }
                mongoService = storage
                return true
            }
        }
    }

    internal suspend fun disableServices(): Boolean {
        var success = true
        mongoService?.let {
            if (!it.shutdown()) {
                DataKache.logger.severe("Failed to shutdown MongoDatabaseService!")
                success = false
            }
        }
        mongoService = null

        return success
    }

    // ------------------------------------------------------------ //
    //                  DATABASE SERVICE MANAGEMENT                 //
    // ------------------------------------------------------------ //
    private var mongoService: MongoDatabaseService? = null
    private fun getMongoService(): MongoDatabaseService {
        require(this == MONGODB) {
            "MongoDatabaseService is only permitted in MONGODB storage mode!"
        }
        return requireNotNull(mongoService) {
            "MongoDatabaseService is not initialized. Ensure that enableServices() was called!"
        }
    }
}
