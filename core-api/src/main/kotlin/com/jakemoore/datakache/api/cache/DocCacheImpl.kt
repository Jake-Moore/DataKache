package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.api.result.RejectableResult
import com.jakemoore.datakache.api.result.handler.DeleteResultHandler
import com.jakemoore.datakache.api.result.handler.ReadResultHandler
import com.jakemoore.datakache.api.result.handler.RejectableUpdateResultHandler
import com.jakemoore.datakache.api.result.handler.UpdateResultHandler
import org.jetbrains.annotations.ApiStatus
import java.util.concurrent.ConcurrentHashMap
import kotlin.properties.Delegates

abstract class DocCacheImpl<K : Any, D : Doc<K, D>>(
    override val cacheName: String,
    override val registration: DataKacheRegistration,
    override val docClass: Class<D>,
    /**
     * @param String - the cache name
     */
    private val loggerInstantiator: (String) -> LoggerService,
) : DocCache<K, D> {
    internal var startedEpochMS by Delegates.notNull<Long>()

    override val databaseName: String
        get() = registration.databaseName

    // ------------------------------------------------------------ //
    //                         Service Methods                      //
    // ------------------------------------------------------------ //
    var running: Boolean = false

    /**
     * Internal method, which should only be called by [DataKacheRegistration.registerDocCache]
     *
     * @return If this call started the service (false if already running)
     */
    @Suppress("RedundantSuspendModifier")
    internal suspend fun start(): Boolean {
        if (running) return false

        // TODO: we should run all of the pre-loading logic
        //  and also register the stream that pulls updates from the database.

        startedEpochMS = System.currentTimeMillis()
        return true
    }

    /**
     * Internal method, which should only be called from [DataKacheRegistration.shutdown]
     *
     * @return If this call shutdown the service (false if already stopped)
     */
    internal suspend fun shutdown(): Boolean {
        if (!running) return false

        if (!shutdownSuper()) {
            getLoggerInternal().error("Failed to shutdown super cache: $cacheName")
            return false
        }

        // Unregister the cache
        registration.onDocCacheShutdown(this)
        return true
    }
    protected abstract suspend fun shutdownSuper(): Boolean

    // ------------------------------------------------------------ //
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //
    private val cacheMap: MutableMap<K, D> = ConcurrentHashMap()

    override fun read(key: K): OptionalResult<D> {
        return ReadResultHandler.wrap {
            return@wrap cacheMap[key]
        }
    }

    @Throws(DocumentNotFoundException::class)
    override suspend fun update(key: K, updateFunction: (D) -> D): DefiniteResult<D> {
        return UpdateResultHandler.wrap {
            return@wrap updateInternal(key, updateFunction)
        }
    }

    @Throws(DocumentNotFoundException::class)
    override suspend fun updateRejectable(key: K, updateFunction: (D) -> D): RejectableResult<D> {
        return RejectableUpdateResultHandler.wrap {
            return@wrap updateInternal(key, updateFunction)
        }
    }

    @Throws(DocumentNotFoundException::class)
    private suspend fun updateInternal(key: K, updateFunction: (D) -> D): D {
        val doc = cacheMap[key] ?: throw DocumentNotFoundException(key, this)
        return DataKache.storageMode.databaseService.update(this, doc, updateFunction)
    }

    override suspend fun delete(key: K): DefiniteResult<Boolean> {
        return DeleteResultHandler.wrap {
            val found = cacheMap.remove(key) != null
            DataKache.storageMode.databaseService.delete(this, key)
            cacheMap.remove(key)

            // This method boolean is whether the document was found in the cache
            //   false is okay, just indicates that the document was not cached
            return@wrap found
        }
    }

    override fun isCached(key: K): Boolean {
        return cacheMap.containsKey(key)
    }

    // ------------------------------------------------------------ //
    //                     Internal Cache Methods                   //
    // ------------------------------------------------------------ //
    @ApiStatus.Internal
    override fun cacheInternal(doc: D) {
        cacheMap[doc.key] = doc
        getLoggerInternal().debug("Cached document: ${doc.key}")
        doc.initializeInternal(this)
    }

    @ApiStatus.Internal
    override fun uncacheInternal(doc: D): Boolean {
        return cacheMap.remove(doc.key) != null
    }

    @ApiStatus.Internal
    override fun uncacheInternal(key: K): Boolean {
        return cacheMap.remove(key) != null
    }

    /**
     * @return The same [doc] for chaining.
     */
    @ApiStatus.Internal
    suspend fun saveDatabaseInternal(doc: D): D {
        // Save the document to the database
        DataKache.storageMode.databaseService.save(this, doc)
        // Cache the document in memory
        this.cacheInternal(doc)
        return doc
    }

    // ------------------------------------------------------------ //
    //                      Cache Logger Service                    //
    // ------------------------------------------------------------ //
    private var _loggerService: LoggerService? = null

    @ApiStatus.Internal
    override fun getLoggerInternal(): LoggerService {
        val service = _loggerService
        if (service != null) {
            return service
        }
        return loggerInstantiator(this.cacheName).also {
            this._loggerService = it
        }
    }
}
