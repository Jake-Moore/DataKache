package com.jakemoore.datakache.api.result.handler

import com.jakemoore.datakache.api.metrics.DataKacheMetrics
import com.jakemoore.datakache.api.metrics.MetricsReceiver
import com.jakemoore.datakache.api.result.DefiniteResult
import com.jakemoore.datakache.api.result.Failure
import com.jakemoore.datakache.api.result.Success
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper

internal object DeleteResultHandler {
    internal suspend fun wrap(
        // Work returns either success or failure (true or false) no other state is allowed.
        work: suspend () -> Boolean
    ): DefiniteResult<Boolean> {
        try {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onDocDelete)

            val value = work()
            return Success(value)
        } catch (e: Exception) {
            // METRICS
            DataKacheMetrics.getReceiversInternal().forEach(MetricsReceiver::onDocDeleteFail)

            return Failure(ResultExceptionWrapper("Delete operation failed.", e))
        }
    }
}
