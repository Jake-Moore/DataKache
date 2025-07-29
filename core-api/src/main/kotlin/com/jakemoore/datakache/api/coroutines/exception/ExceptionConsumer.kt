package com.jakemoore.datakache.api.coroutines.exception

import com.jakemoore.datakache.api.coroutines.DataKacheScope

/**
 * Consumer interface for handling exceptions from [DataKacheScope] coroutines.
 */
interface ExceptionConsumer {
    fun accept(exception: Throwable)
}
