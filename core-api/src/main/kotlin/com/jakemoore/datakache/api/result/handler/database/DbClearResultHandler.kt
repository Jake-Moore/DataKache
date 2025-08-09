package com.jakemoore.datakache.api.result.handler.database

import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper

internal object DbClearResultHandler {
    internal suspend fun wrap(
        // Work returns a nonnull Long value on success, or throws an exception on failure.
        work: suspend () -> Long
    ): DefiniteResult<Long> {
        try {
            val value = work()
            return Success(value)
        } catch (e: Exception) {
            return Failure(ResultExceptionWrapper("DB Clear operation failed.", e))
        }
    }
}
