package com.jakemoore.datakache.api.exception.doc

import com.jakemoore.datakache.api.exception.update.DocumentUpdateException

/**
 * Exception thrown when one of the copy helper methods for a document produces an invalid result.
 *
 * For example, if the document returned by [com.jakemoore.datakache.api.doc.Doc.copyHelper] does not
 * match the expected version asked for.
 *
 * @param docNamespace The namespace of the document that caused the exception.
 * @param message The detail message explaining the exception.
 */
@Suppress("unused")
class InvalidDocCopyHelperException(
    val docNamespace: String,
    message: String,
) : DocumentUpdateException("[$docNamespace] $message")
