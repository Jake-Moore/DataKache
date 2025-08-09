@file:Suppress("unused")

package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.exception.data.Operation

class DuplicateUniqueIndexException(
    val docCache: DocCache<*, *>,
    val index: String,
    val operation: Operation,
    val fullMessage: String,
    cause: Throwable,
) : DataKacheException(
    message = "Duplicate Unique Index Exception: ${operation.name} operation failed " +
        "on ${docCache.cacheName} collection. " +
        "Unique index '$index' constraint violated. MongoDB error: $fullMessage",
    cause = cause,
)
