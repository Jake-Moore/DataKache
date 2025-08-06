package com.jakemoore.datakache.api.result.handler

import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiver
import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper

internal object ReadUniqueIndexResultHandler {
    internal fun <K : Any, D : Doc<K, D>> wrap(
        // Work may return a null document, which indicates that the document was not found.
        work: () -> D?
    ): OptionalResult<D> {
        try {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onCacheReadDocByUniqueIndex)

            val value = work()
            return if (value != null) {
                Success(value)
            } else {
                Empty()
            }
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onCacheReadDocByUniqueIndexFail)

            return Failure(
                ResultExceptionWrapper(
                    "Unique Index Read operation failed.",
                    e
                )
            )
        }
    }
}
