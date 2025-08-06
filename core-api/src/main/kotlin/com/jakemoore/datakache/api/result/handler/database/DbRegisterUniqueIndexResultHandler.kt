package com.jakemoore.datakache.api.result.handler.database

import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper

internal object DbRegisterUniqueIndexResultHandler {
    internal suspend fun wrap(
        // Work may return a null document, which indicates that the document was not found.
        work: suspend () -> Unit
    ): DefiniteResult<Unit> {
        try {
            work()
            return Success(Unit)
        } catch (e: Exception) {
            return Failure(
                ResultExceptionWrapper(
                    "DB Register Unique Index operation failed.",
                    e
                )
            )
        }
    }
}
