package com.jakemoore.datakache.api.exception.update

import com.jakemoore.datakache.api.exception.DataKacheException

// DocumentUpdateExceptions.kt
open class DocumentUpdateException(message: String) : DataKacheException(message)

/** Thrown when the update function returns the _same_ instance. */
class UpdateFunctionReturnedSameInstanceException(
    val docNamespace: String
) : DocumentUpdateException(
    "[$docNamespace] Update function must return a new doc (using data class copy)"
)

/** Thrown when the key of the updated document doesn’t match the expected key. */
class IllegalDocumentKeyModificationException(
    val docNamespace: String,
    val foundKeyString: String,
    val expectedKeyString: String
) : DocumentUpdateException(
    "[$docNamespace] Updated doc key mismatch! Found: $foundKeyString, Expected: $expectedKeyString"
)

/** Thrown when the version on the updated document isn’t the next optimistic version. */
class IllegalDocumentVersionModificationException(
    val docNamespace: String,
    val foundVersion: Long,
    val expectedVersion: Long
) : DocumentUpdateException(
    "[$docNamespace] Updated doc version mismatch! Found: $foundVersion, Expected: $expectedVersion"
)
