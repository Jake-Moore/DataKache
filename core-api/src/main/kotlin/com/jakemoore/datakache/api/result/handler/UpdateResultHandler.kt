package com.jakemoore.datakache.api.result.handler

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiver
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper

internal object UpdateResultHandler {
    internal suspend fun <K : Any, D : Doc<K, D>> wrap(
        // Work cannot return a null document.
        //   If the document is not found it should throw a [DocumentNotFoundException].
        work: suspend () -> D
    ): DefiniteResult<D> {
        try {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onDocUpdate)

            val value = work()
            return Success(requireNotNull(value))
        } catch (e: DocumentNotFoundException) {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onDocUpdateNotFoundFail)

            // In the middle of this update we discovered that the document does not exist.
            // (it was likely deleted by another thread or task before this operation could complete)
            return Failure(
                ResultExceptionWrapper(
                    message = "Update operation failed: Document not found",
                    exception = e,
                )
            )
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onDocUpdateFail)

            return Failure(ResultExceptionWrapper("Update operation failed.", e))
        }
    }
}
