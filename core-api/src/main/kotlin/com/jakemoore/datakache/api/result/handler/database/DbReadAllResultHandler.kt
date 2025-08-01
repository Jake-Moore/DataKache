package com.jakemoore.datakache.api.result.handler.database

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper
import kotlinx.coroutines.flow.Flow

internal object DbReadAllResultHandler {
    internal suspend fun <K : Any, D : Doc<K, D>> wrap(
        // Work may return a null document, which indicates that the document was not found.
        work: suspend () -> Flow<D>
    ): DefiniteResult<Flow<D>> {
        return try {
            Success(work())
        } catch (e: Exception) {
            Failure(ResultExceptionWrapper("DB Read All operation failed.", e))
        }
    }
}
