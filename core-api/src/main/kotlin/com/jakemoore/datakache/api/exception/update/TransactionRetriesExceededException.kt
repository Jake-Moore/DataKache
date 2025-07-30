package com.jakemoore.datakache.api.exception.update

import com.jakemoore.datakache.api.exception.DataKacheException

class TransactionRetriesExceededException(
    override val message: String,
    override val cause: Throwable? = null,
) : DataKacheException(message, cause)
