package com.jakemoore.datakache.api.result.handler

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper

internal object DeleteResultHandler {
    internal suspend fun <K : Any, D : Doc<K, D>> wrap(
        // Work returns either success or failure (true or false) no other state is allowed.
        work: suspend () -> Boolean
    ): DefiniteResult<Boolean> {
        try {
            val value = work()
            return Success(requireNotNull(value))
        } catch (t: Throwable) {
            return Failure(ResultExceptionWrapper("Delete operation failed.", t))
        }
    }
}
