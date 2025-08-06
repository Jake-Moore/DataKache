package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.DataKacheClient
import com.jakemoore.datakache.api.registration.DatabaseRegistration

/**
 * @param registration The existing database registration that caused the conflict.
 * @param client The new client attempting to register the same database.
 */
@Suppress("unused")
class DuplicateDatabaseException(registration: DatabaseRegistration, client: DataKacheClient) : DataKacheException(
    "The database named '${registration.databaseName}' could not be registered by plugin " +
        "'${client.name}' because it has already been registered by plugin " +
        "'${registration.parentClient.name}'."
)
