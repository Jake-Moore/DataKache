@file:Suppress("unused")

package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.exception.data.Operation

class DuplicateDocumentKeyException(
    val docCache: DocCache<*, *>,
    val keyString: String,
    val operation: Operation,
    val fullMessage: String,
    cause: Throwable,
) : DataKacheException(
    message = "Duplicate Document Key '$keyString' during '${operation.name}': " +
        fullMessage.ifBlank { "Operation Failed." },
    cause = cause,
)
