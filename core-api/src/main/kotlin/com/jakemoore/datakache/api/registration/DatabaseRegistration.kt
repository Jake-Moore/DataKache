package com.jakemoore.datakache.api.registration

import com.jakemoore.datakache.api.DataKacheClient

/**
 * Represents a registered database in DataKache.
 *
 * This class holds the database name and the client that registered it.
 */
data class DatabaseRegistration(
    val databaseName: String, // Full Database name (case sensitive)
    val parentClient: DataKacheClient, // Client that registered this database
)
