package com.jakemoore.datakache.util

import com.jakemoore.datakache.api.DataKacheAPI
import com.jakemoore.datakache.api.DataKacheClient
import com.jakemoore.datakache.api.client.DefaultKacheClient
import com.jakemoore.datakache.api.mode.StorageMode
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.util.core.container.DataKacheTestContainer
import com.jakemoore.datakache.util.core.container.MongoDataKacheTestContainer
import com.jakemoore.datakache.util.doc.TestGenericDocCache

object TestUtil {

    /**
     * Returns a test container for the specified database type.
     *
     * @param storageMode The type of database to use for testing
     * @param databaseName The name of the test database
     * @return A DataKacheTestContainer instance for the specified database type
     */
    fun createTestContainer(
        storageMode: StorageMode,
        databaseName: String = "TestDatabase"
    ): DataKacheTestContainer {
        return when (storageMode) {
            StorageMode.MONGODB -> MongoDataKacheTestContainer(databaseName)
        }
    }

    /**
     * Starts the necessary test containers for the specified storage mode.
     *
     * Should be called before all tests to ensure the environment is set up correctly.
     */
    fun startTestContainers(
        storageMode: StorageMode,
    ) {
        when (storageMode) {
            StorageMode.MONGODB -> MongoDataKacheTestContainer.startContainers()
        }
    }

    /**
     * Stops the test containers for the specified storage mode.
     *
     * Should be called after all tests to clean up the environment.
     */
    fun stopTestContainers(
        storageMode: StorageMode,
    ) {
        when (storageMode) {
            StorageMode.MONGODB -> MongoDataKacheTestContainer.stopContainers()
        }
    }

    fun createRegistration(
        client: DataKacheClient = DefaultKacheClient("TestClient"),
        databaseName: String = "TestDatabase",
    ): DataKacheRegistration {
        return DataKacheAPI.register(
            client = client,
            databaseName = databaseName,
        )
    }

    suspend fun createTestGenericDocCache(
        registration: DataKacheRegistration,
    ): TestGenericDocCache {
        return TestGenericDocCache(registration).also {
            registration.registerDocCache(it)
        }
    }
}
