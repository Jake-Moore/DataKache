@file:Suppress("unused")

package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.exception.data.Operation

class DocumentNotFoundException(
    val keyString: String,
    val docCache: DocCache<*, *>,
    val operation: Operation,
) : DataKacheException(
    "No document found with key '$keyString' in DocCache '${docCache.cacheName}'" +
        " during operation '${operation.name}'."
)
