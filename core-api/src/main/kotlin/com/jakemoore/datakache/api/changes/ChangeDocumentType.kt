package com.jakemoore.datakache.api.changes

import kotlin.jvm.Throws

/**
 * Database-agnostic operation types for change streams.
 *
 * This enum represents the subtypes of [ChangeOperationType] which involve an updated document object.
 *
 * Document operations:
 * - INSERT: New document created
 * - UPDATE: Document modified (partial update)
 * - REPLACE: Document completely replaced
 */
enum class ChangeDocumentType {
    INSERT,
    UPDATE,
    REPLACE,
    ;

    companion object {
        @Throws(IllegalArgumentException::class)
        fun fromOperationType(type: ChangeOperationType): ChangeDocumentType {
            return when (type) {
                ChangeOperationType.INSERT -> INSERT
                ChangeOperationType.UPDATE -> UPDATE
                ChangeOperationType.REPLACE -> REPLACE
                else -> throw IllegalArgumentException("Unsupported ChangeOperationType: $type")
            }
        }
    }
}
