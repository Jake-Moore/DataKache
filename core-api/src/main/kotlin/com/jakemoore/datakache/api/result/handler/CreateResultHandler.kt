package com.jakemoore.datakache.api.result.handler

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper
import com.mongodb.DuplicateKeyException
import org.jetbrains.annotations.ApiStatus

@ApiStatus.Internal
object CreateResultHandler {
    @ApiStatus.Internal
    suspend fun <K : Any, D : Doc<K, D>> wrap(
        // Work cannot return a null document.
        //   If the document has a key conflict, [DuplicateKeyException] should be thrown.
        work: suspend () -> D
    ): DefiniteResult<D> {
        try {
            val value = work()
            return Success(requireNotNull(value))
        } catch (d: DuplicateKeyException) {
            // We tried to create a document, but a document with this key already exists.
            return Failure(
                ResultExceptionWrapper(
                    message = "Create operation failed: Key already exists!",
                    exception = d,
                )
            )
        } catch (t: Throwable) {
            return Failure(ResultExceptionWrapper("Create operation failed.", t))
        }
    }
}
