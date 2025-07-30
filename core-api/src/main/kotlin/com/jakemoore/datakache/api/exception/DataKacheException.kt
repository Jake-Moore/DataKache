package com.jakemoore.datakache.api.exception

/**
 * Base [Exception] class for ALL of DataKache
 */
open class DataKacheException(
    override val message: String,
    override val cause: Throwable? = null,
) : Exception(message, cause)
