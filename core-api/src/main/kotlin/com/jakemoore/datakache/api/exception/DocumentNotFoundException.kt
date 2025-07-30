@file:Suppress("unused")

package com.jakemoore.datakache.api.exception

import com.jakemoore.datakache.api.cache.DocCache

class DocumentNotFoundException(
    val key: Any,
    val docCache: DocCache<*, *>,
) : NoSuchElementException("No document found with key=$key")
