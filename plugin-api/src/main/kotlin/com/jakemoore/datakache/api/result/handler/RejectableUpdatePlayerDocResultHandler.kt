package com.jakemoore.datakache.api.result.handler

import com.jakemoore.datakache.api.doc.PlayerDoc
import com.jakemoore.datakache.api.exception.DocumentNotFoundException
import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiver
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Reject
import com.jakemoore.datakache.api.result.RejectableResult
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper
import kotlinx.coroutines.CancellationException

internal object RejectableUpdatePlayerDocResultHandler {
    internal suspend fun <D : PlayerDoc<D>> wrap(
        // Work cannot return a null document.
        //   If the document is not found it should throw a [DocumentNotFoundException].
        work: suspend () -> D
    ): RejectableResult<D> {
        try {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onDocRejectableUpdate)

            val value = work()
            return Success(value)
        } catch (e: CancellationException) {
            // propagate cancellation exceptions
            throw e
        } catch (e: DocumentNotFoundException) {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onDocRejectableUpdateNotFoundFail)

            // In the middle of this update we discovered that the document does not exist.
            // (it was likely deleted by another thread or task before this operation could complete)
            return Failure(
                ResultExceptionWrapper(
                    message = "Update operation failed: Document not found",
                    exception = e,
                )
            )
        } catch (e: RejectUpdateException) {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onDocRejectableUpdateRejected)

            return Reject(e)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onDocRejectableUpdateFail)

            return Failure(ResultExceptionWrapper("Rejectable Update operation failed.", e))
        }
    }
}
