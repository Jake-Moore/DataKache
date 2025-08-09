package com.jakemoore.datakache.api.exception.update

/** Thrown when the username of the updated document doesn’t match the expected username. */
class IllegalDocumentUsernameModificationException(
    val docNamespace: String,
    val foundUsername: String?,
    val expectedUsername: String?,
) : DocumentUpdateException(
    "[$docNamespace] Updated doc username mismatch! Found: '$foundUsername', Expected: '$expectedUsername'"
)
