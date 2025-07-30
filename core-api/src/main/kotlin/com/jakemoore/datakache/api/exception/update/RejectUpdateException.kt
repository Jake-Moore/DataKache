package com.jakemoore.datakache.api.exception.update

import com.jakemoore.datakache.api.exception.DataKacheException

/**
 * Intended to be thrown inside a rejectable [com.jakemoore.datakache.api.doc.Doc] update operation.
 */
open class RejectUpdateException(
    override val message: String,
    override val cause: Throwable? = null,
) : DataKacheException(message, cause)
