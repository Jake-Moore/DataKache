package com.jakemoore.datakache.api.result.handler.database

import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper
import kotlinx.coroutines.flow.Flow

internal object DbReadKeysResultHandler {
    internal suspend fun <K : Any> wrap(
        // Work may return a null document, which indicates that the document was not found.
        work: suspend () -> Flow<K>
    ): DefiniteResult<Flow<K>> {
        return try {
            Success(work())
        } catch (e: Exception) {
            Failure(ResultExceptionWrapper("DB Read Keys operation failed.", e))
        }
    }
}
