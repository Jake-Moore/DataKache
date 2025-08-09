@file:Suppress("unused")

package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.exception.data.Operation

class DuplicateUniqueIndexException(
    val docCache: DocCache<*, *>,
    val fullMessage: String,
    val index: String,
    val operation: Operation,
) : DataKacheException(
    "Duplicate Unique Index Exception: ${operation.name} operation failed " +
        "on ${docCache.cacheName} collection. " +
        "Unique index '$index' constraint violated. MongoDB error: $fullMessage"
)
