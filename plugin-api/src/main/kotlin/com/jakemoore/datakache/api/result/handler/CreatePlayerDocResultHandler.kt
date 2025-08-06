package com.jakemoore.datakache.api.result.handler

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiver
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper
import org.jetbrains.annotations.ApiStatus

@ApiStatus.Internal
internal object CreatePlayerDocResultHandler {
    @ApiStatus.Internal
    suspend fun <K : Any, D : Doc<K, D>> wrap(
        // Work cannot return a null document.
        //   If the document has a key conflict, [DuplicateDocumentKeyException] should be thrown.
        //   If the document has a unique index conflict, [DuplicateUniqueIndexException] should be thrown.
        work: suspend () -> D
    ): DefiniteResult<D> {
        try {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onPlayerDocCreate)

            val value = work()
            return Success(requireNotNull(value))
        } catch (d: DuplicateDocumentKeyException) {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onPlayerDocCreateDuplicateFail)

            // We tried to create a document, but a document with this key already exists.
            return Failure(
                ResultExceptionWrapper(
                    message = "Create operation failed: Key already exists!",
                    exception = d,
                )
            )
        } catch (d: DuplicateUniqueIndexException) {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onPlayerDocCreateDuplicateFailIndex)

            // We tried to create a document, but a document with this unique index already exists.
            return Failure(
                ResultExceptionWrapper(
                    message = "Create operation failed: Unique index already exists!",
                    exception = d,
                )
            )
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onPlayerDocCreateFail)

            return Failure(
                ResultExceptionWrapper(
                    "Create PlayerDoc operation failed.",
                    e
                )
            )
        }
    }
}
