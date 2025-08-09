package com.jakemoore.datakache.api.exception.data

/**
 * Identifies the type of data operation for exception and logging context.
 *
 * This applies to operations on [com.jakemoore.datakache.api.doc.Doc]s
 * or [com.jakemoore.datakache.api.cache.DocCache]s.
 *
 */
enum class Operation {
    CREATE,
    READ,
    UPDATE,
    UPDATE_REJECTABLE,
    DELETE,
    REPLACE,
}
