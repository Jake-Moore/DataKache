@file:Suppress("unused")

package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.cache.DocCache

class DuplicateDocumentKeyException(
    val docCache: DocCache<*, *>,
    val keyString: String,
    val fullMessage: String,
    val operation: String? = null,
) : DataKacheException("Duplicate Document Key '$keyString' Exception: Operation Failed.")
