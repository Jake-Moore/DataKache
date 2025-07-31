package com.jakemoore.datakache.core.connections.changes

/**
 * Database-agnostic operation types for change streams.
 */
enum class ChangeOperationType {
    INSERT,
    UPDATE,
    REPLACE,
    DELETE,
    DROP,
    RENAME,
    DROP_DATABASE,
    INVALIDATE,
}
