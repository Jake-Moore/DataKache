package com.jakemoore.datakache.api.result.handler.database

import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper

internal object DbHasKeyResultHandler {
    internal suspend fun wrap(
        // Work returns either success or failure (true or false) no other state is allowed.
        work: suspend () -> Boolean
    ): DefiniteResult<Boolean> {
        try {
            val value = work()
            return Success(requireNotNull(value))
        } catch (e: Exception) {
            return Failure(ResultExceptionWrapper("DB Has Key operation failed.", e))
        }
    }
}
