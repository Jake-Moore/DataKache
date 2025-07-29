package com.jakemoore.datakache.api.registration

import com.jakemoore.datakache.api.DataKacheAPI
import com.jakemoore.datakache.api.DataKacheClient

// Internal constructor, instantiation is managed by DataKache
@Suppress("unused")
class DataKacheRegistration internal constructor(
    val client: DataKacheClient,
    dbNameShort: String,
) {

    /**
     * The full database name as it would appear in MongoDB
     * This includes the DataKache prefix, described in [DataKacheAPI.getFullDatabaseName] (String)}
     * All plugin collections will be stored in this database
     */
    val databaseName: String
    private val dbNameShort: String

    init {
        require(dbNameShort.isNotBlank()) {
            "Database name cannot be blank."
        }

        this.dbNameShort = dbNameShort
        this.databaseName = DataKacheAPI.getFullDatabaseName(dbNameShort)
    }

    @Suppress("RedundantSuspendModifier") // TODO
    suspend fun shutdown() {
        DataKacheAPI.registrations.remove(this)
    }
}
