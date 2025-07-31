package com.jakemoore.datakache.core.connections.changes

/**
 * Database-agnostic operation types for change streams.
 *
 * Document operations:
 * - INSERT: New document created
 * - UPDATE: Document modified (partial update)
 * - REPLACE: Document completely replaced
 * - DELETE: Document removed
 *
 * Administrative operations:
 * - DROP: Collection dropped
 * - RENAME: Collection renamed
 * - DROP_DATABASE: Database dropped
 * - INVALIDATE: Change stream invalidated
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
