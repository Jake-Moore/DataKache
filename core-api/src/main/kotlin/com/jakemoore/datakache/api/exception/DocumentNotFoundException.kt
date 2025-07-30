package com.jakemoore.datakache.api.exception

class DocumentNotFoundException(
    id: Any
) : NoSuchElementException("No document found with id=$id")
