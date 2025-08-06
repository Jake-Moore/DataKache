package com.jakemoore.datakache.api.result.handler

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiver
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper
import com.mongodb.DuplicateKeyException
import org.jetbrains.annotations.ApiStatus

@ApiStatus.Internal
internal object CreatePlayerDocResultHandler {
    @ApiStatus.Internal
    suspend fun <K : Any, D : Doc<K, D>> wrap(
        // Work cannot return a null document.
        //   If the document has a key conflict, [DuplicateKeyException] should be thrown.
        work: suspend () -> D
    ): DefiniteResult<D> {
        try {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onPlayerDocCreate)

            val value = work()
            return Success(requireNotNull(value))
        } catch (d: DuplicateKeyException) {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onPlayerDocCreateFail)

            // We tried to create a document, but a document with this key already exists.
            return Failure(
                ResultExceptionWrapper(
                    message = "Create operation failed: Key already exists!",
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
