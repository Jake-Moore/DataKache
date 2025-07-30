package com.jakemoore.datakache.api.cache

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.api.result.handler.ReadResultHandler
import org.jetbrains.annotations.ApiStatus
import java.util.concurrent.ConcurrentHashMap

abstract class DocCacheImpl<K : Any, D : Doc<K, D>>(
    override val nickname: String,
    override val registration: DataKacheRegistration,
    override val docClass: Class<D>,
    /**
     * @param String - the cache nickname
     */
    private val loggerInstantiator: (String) -> LoggerService,
) : DocCache<K, D> {

    override val databaseName: String
        get() = registration.databaseName

    // ------------------------------------------------------------ //
    //                         Service Methods                      //
    // ------------------------------------------------------------ //
    override var running: Boolean = false

    // ------------------------------------------------------------ //
    //                          CRUD Methods                        //
    // ------------------------------------------------------------ //
    private val cacheMap: MutableMap<K, D> = ConcurrentHashMap()

    override fun read(key: K): OptionalResult<D> {
        return ReadResultHandler.wrap {
            return@wrap cacheMap[key]
        }
    }

    // TODO finish crud methods

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
        return loggerInstantiator(this.nickname).also {
            this._loggerService = it
        }
    }
}
