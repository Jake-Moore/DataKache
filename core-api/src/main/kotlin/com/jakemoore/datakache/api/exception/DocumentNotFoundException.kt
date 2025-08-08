@file:Suppress("unused")

package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.cache.DocCache

class DocumentNotFoundException(
    val keyString: String,
    val docCache: DocCache<*, *>,
    val operation: String,
) : DataKacheException(
    "No document found with key '$keyString' in DocCache '${docCache.cacheName}'."
)
