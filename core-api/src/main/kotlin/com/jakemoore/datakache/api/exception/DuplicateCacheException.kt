package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.registration.DatabaseRegistration
import kotlin.reflect.KClass

/**
 * @param database The database where the cache name conflict occurred.
 */
@Suppress("unused")
class DuplicateCacheException(
    database: DatabaseRegistration,
    cacheName: String,
    existingCacheClass: KClass<out DocCache<*, *>>,
    newCacheClass: KClass<out DocCache<*, *>>,
) : Exception(
    "The database named '${database.databaseName}' (registered by ${database.parentClient.name}) " +
        "failed to register a Cache: '$cacheName' because it has already been registered. " +
        "It was previously registered by ${existingCacheClass.simpleName}, " +
        "and the following cache tried to register it again: ${newCacheClass.simpleName}. "
)
