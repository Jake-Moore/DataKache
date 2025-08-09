package com.jakemoore.datakache.api.registration

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.DataKacheAPI
import com.jakemoore.datakache.api.DataKacheClient
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.cache.DocCacheImpl
import com.jakemoore.datakache.api.exception.DuplicateCacheException
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

/**
 * This class represents a registration for a DataKache database.
 *
 * It is used to access information about the database registration.
 *
 * It also maintains all registered document caches for this database.
 */
@Suppress("unused")
class DataKacheRegistration internal constructor(
    // Internal constructor, instantiation is managed by DataKache
    /**
     * Your DataKache client instance
     */
    val client: DataKacheClient,
    /**
     * The full database name as it would appear in MongoDB
     * This includes the DataKache namespace, described in [DataKacheAPI.getFullDatabaseName] (String)}
     * All collections will be stored in this database
     */
    val databaseName: String,
    /**
     * The [DatabaseRegistration] for this registration.
     */
    val databaseRegistration: DatabaseRegistration,
) {

    /**
     * API Method to safely shut down this registration and all associated caches.
     *
     * This should be called at the end of your software's lifecycle to ensure proper cleanup.
     */
    suspend fun shutdown() {
        // Iterate through all document caches and shut them down
        //  Wrapped in an array to prevent concurrent modification issues (cache.shutdown() removes itself from the map)
        ArrayList(docCaches.values).forEach { cache ->
            val success = cache.shutdown()

            if (!success) {
                DataKache.logger.severe(
                    "Failed to shut down cache '${cache.cacheName}' in database '$databaseName'. " +
                        "Please check the cache implementation for proper shutdown handling."
                )
            }
        }
        // Clear any dangling caches (this should not occur, but is a safety measure)
        docCaches.clear()

        // Remove this registration from the API records
        DataKacheAPI.shutdown(this)
    }

    // ------------------------------------------------------------ //
    //                       DocCache Methods                       //
    // ------------------------------------------------------------ //
    internal val docCaches = ConcurrentHashMap<String, DocCacheImpl<*, *>>() // Map<CacheName, DocCache>

    /**
     * Registers a new document cache for this database.
     *
     * @param docCache Your custom [DocCache] that has been instantiated with the appropriate type parameters.
     */
    @Throws(DuplicateCacheException::class)
    suspend fun registerDocCache(docCache: DocCacheImpl<*, *>) {
        val key = normalizeCacheName(docCache.cacheName)

        // Check if a cache with the same name already exists
        val existingCache: DocCache<*, *>? = docCaches[key]
        if (existingCache != null) {
            throw DuplicateCacheException(
                databaseRegistration,
                docCache.cacheName,
                existingCache::class,
                docCache::class,
            )
        }

        // Enable the cache (will handle preload and initialization)
        docCache.start()

        // Register the new cache
        docCaches[key] = docCache
        docCache.getLoggerInternal().info("Registered Cache in Database: $databaseName")
    }

    internal fun onDocCacheShutdown(docCache: DocCache<*, *>) {
        val key = normalizeCacheName(docCache.cacheName)

        // Remove the cache from the map
        docCaches.remove(key)
    }

    // Normalizes the cache name to ensure uniqueness and consistency in the docCaches map.
    private fun normalizeCacheName(cacheName: String): String {
        return cacheName.lowercase()
            .replace(" ", "_") // replace spaces with underscores
            .replace("\\p{Zs}+".toRegex(), "") // replace any other whitespace
    }

    /**
     * Gets a read only collection of all registered document caches for this database.
     */
    fun getDocCaches(): Collection<DocCache<*, *>> {
        return Collections.unmodifiableCollection(docCaches.values)
    }
}
