package com.jakemoore.datakache.api.registration

import com.jakemoore.datakache.api.DataKacheClient

data class DatabaseRegistration(
    val databaseName: String, // Full Database name (case sensitive)
    val parentClient: DataKacheClient, // Client that registered this database
)
