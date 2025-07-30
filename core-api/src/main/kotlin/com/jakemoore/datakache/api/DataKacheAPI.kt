@file:Suppress("unused")

package com.jakemoore.datakache.api

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.exception.DuplicateDatabaseException
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.registration.DatabaseRegistration
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Main API class for DataKache. Register data collections here.
 */
object DataKacheAPI {
    // Need concurrency safety, but also there should not be many writes to this list.
    internal val registrations = CopyOnWriteArrayList<DataKacheRegistration>()

    private val databasePrefix: String
        get() = DataKache.databasePrefix

    // ---------------------------------------- //
    //          Registration Methods            //
    // ---------------------------------------- //
    /**
     * Register your plugin with a unique MongoDB database name.
     *
     * @return A reusable [DataKacheRegistration] instance to be provided to your collections.
     *
     * @throws DuplicateDatabaseException if the database name is already in use.
     */
    @Throws(DuplicateDatabaseException::class)
    fun register(client: DataKacheClient, databaseName: String): DataKacheRegistration {
        require(databaseName.isNotBlank()) {
            "Database name cannot be blank."
        }

        registerDatabase(client, getFullDatabaseName(databaseName))
        return DataKacheRegistration(client, databaseName).also {
            registrations.add(it)
        }
    }

    // ---------------------------------------- //
    //            Database Methods              //
    // ---------------------------------------- //
    // Key is databaseName stored lowercase for uniqueness
    internal val databases = ConcurrentHashMap<String, DatabaseRegistration>()

    @Throws(DuplicateDatabaseException::class)
    private fun registerDatabase(client: DataKacheClient, databaseName: String) {
        val registration = getDatabaseRegistration(databaseName)
        if (registration != null) {
            throw DuplicateDatabaseException(registration, client)
        }
        databases[databaseName.lowercase(Locale.getDefault())] = DatabaseRegistration(databaseName, client)
    }

    private fun getDatabaseRegistration(databaseName: String): DatabaseRegistration? {
        return databases[databaseName.lowercase(Locale.getDefault())]
    }

    /**
     * Check if a database name is already registered/taken by a [DataKacheClient] using DataKache.
     */
    fun isDatabaseNameRegistered(databaseName: String): Boolean {
        return databases.containsKey(databaseName.lowercase(Locale.getDefault()))
    }

    /**
     * Appends [DataKacheAPI.databasePrefix] and a '_' char to the beginning of the dbName,
     * to allow one MongoDB instance to be shared by multiple servers running DataKache.
     */
    fun getFullDatabaseName(dbName: String): String {
        require(dbName.isNotBlank()) {
            "Database name cannot be blank."
        }

        // Just in case, don't add the prefix twice
        val prefix = databasePrefix
        if (dbName.startsWith(prefix + "_")) {
            return dbName
        }
        return "${prefix}_$dbName"
    }
}
