package com.jakemoore.datakache.api.result.handler.database

import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper

internal object DbSizeResultHandler {
    internal suspend fun wrap(
        // Work returns either success or failure (true or false) no other state is allowed.
        work: suspend () -> Long
    ): DefiniteResult<Long> {
        try {
            val value = work()
            return Success(requireNotNull(value))
        } catch (e: Exception) {
            return Failure(ResultExceptionWrapper("DB Size operation failed.", e))
        }
    }
}
