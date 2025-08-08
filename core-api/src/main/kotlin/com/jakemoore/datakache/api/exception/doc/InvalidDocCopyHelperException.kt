package com.jakemoore.datakache.api.exception.doc

/**
 * Exception thrown when one of the copy helper methods for a document produces an invalid result.
 *
 * For example, if the document returned by [com.jakemoore.datakache.api.doc.Doc.copyHelper] does not
 * match the expected version asked for.
 *
 * @param message The detail message explaining the exception.
 */
@Suppress("unused")
class InvalidDocCopyHelperException(
    val docNamespace: String,
    message: String,
) : RuntimeException(message)
