package com.jakemoore.datakache.core.connections.mongo.changestream

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.logging.LoggerService
import com.jakemoore.datakache.core.connections.changes.ChangeEventHandler
import com.jakemoore.datakache.core.connections.changes.ChangeStreamConfig
import com.mongodb.kotlin.client.coroutine.MongoCollection

/**
 * Shared context object containing all dependencies needed by change stream components.
 * This provides a clean way to pass shared state and dependencies between components.
 */
internal data class ChangeStreamContext<K : Any, D : Doc<K, D>>(
    val collection: MongoCollection<D>,
    val eventHandler: ChangeEventHandler<K, D>,
    val config: ChangeStreamConfig,
    private val loggerService: LoggerService
) {
    val logger: LoggerService = object : LoggerService {
        override val loggerName: String
            get() = loggerService.loggerName
        override val permitsDebugStatements: Boolean
            get() = loggerService.permitsDebugStatements

        override fun logToConsole(
            msg: String,
            level: LoggerService.LogLevel
        ) {
            // Add the collection name prefix to our log messages
            val collName = collection.namespace.collectionName
            loggerService.logToConsole("[$collName] $msg", level)
        }
    }
}
