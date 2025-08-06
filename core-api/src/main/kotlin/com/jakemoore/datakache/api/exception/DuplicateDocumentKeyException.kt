@file:Suppress("unused")

package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.cache.DocCache

class DuplicateDocumentKeyException(
    val docCache: DocCache<*, *>,
    val fullMessage: String,
    val operation: String? = null,
) : DataKacheException("Duplicate Document Key Exception: Operation Failed.")
