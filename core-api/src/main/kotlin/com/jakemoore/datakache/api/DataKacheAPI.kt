@file:Suppress("unused")

package com.jakemoore.datakache.api

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.exception.DuplicateDatabaseException
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.registration.DatabaseRegistration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Main API class for DataKache. Register data caches here.
 */
object DataKacheAPI {
    // Need concurrency safety, but also there should not be many writes to this list.
    internal val registrations = CopyOnWriteArrayList<DataKacheRegistration>()

    private val databaseNamespace: String
        get() = DataKache.databaseNamespace

    // ---------------------------------------- //
    //          Registration Methods            //
    // ---------------------------------------- //
    /**
     * Register your plugin with a unique MongoDB database name.
     *
     * @return A reusable [DataKacheRegistration] instance to be provided to your caches.
     *
     * @throws DuplicateDatabaseException if the database name is already in use.
     */
    @Throws(DuplicateDatabaseException::class)
    fun register(client: DataKacheClient, databaseName: String): DataKacheRegistration {
        require(databaseName.isNotBlank()) {
            "Database name cannot be blank."
        }

        val databaseRegistration = registerDatabase(client, getFullDatabaseName(databaseName))
        return DataKacheRegistration(client, databaseName, databaseRegistration).also {
            registrations.add(it)
        }
    }

    // ---------------------------------------- //
    //            Database Methods              //
    // ---------------------------------------- //
    // Key is databaseName stored lowercase for uniqueness
    internal val databases = ConcurrentHashMap<String, DatabaseRegistration>()

    @Throws(DuplicateDatabaseException::class)
    private fun registerDatabase(client: DataKacheClient, databaseName: String): DatabaseRegistration {
        val registration = getDatabaseRegistration(databaseName)
        if (registration != null) {
            throw DuplicateDatabaseException(registration, client)
        }
        return DatabaseRegistration(databaseName, client).also {
            databases[databaseName.lowercase()] = it
        }
    }

    private fun getDatabaseRegistration(databaseName: String): DatabaseRegistration? {
        return databases[databaseName.lowercase()]
    }

    /**
     * Check if a database name is already registered/taken by a [DataKacheClient] using DataKache.
     */
    fun isDatabaseNameRegistered(databaseName: String): Boolean {
        return databases.containsKey(databaseName.lowercase())
    }

    /**
     * Appends [DataKacheAPI.databaseNamespace] and a '_' char to the beginning of the dbName,
     * to allow one MongoDB instance to be shared by multiple servers running DataKache.
     */
    fun getFullDatabaseName(dbName: String): String {
        require(dbName.isNotBlank()) {
            "Database name cannot be blank."
        }

        // Just in case, don't add the namespace twice
        val namespace = databaseNamespace
        if (dbName.startsWith(namespace + "_")) {
            return dbName
        }
        return "${namespace}_$dbName"
    }
}
