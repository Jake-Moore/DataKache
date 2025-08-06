@file:Suppress("unused")

package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.cache.DocCache

class DuplicateUniqueIndexException(
    val docCache: DocCache<*, *>,
    val fullMessage: String,
    val operation: String? = null,
) : DataKacheException("Duplicate Unique Index Exception: Operation Failed.")
