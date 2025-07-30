package com.jakemoore.datakache.api.result

import com.jakemoore.datakache.api.exception.update.RejectUpdateException
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper

/**
 * Reject state (when an update is rejected from the updateFunction).
 */
class Reject<T>(
    val exception: RejectUpdateException,
) : RejectableResult<T> {
    /**
     * @return false (not a success)
     */
    override fun isSuccess(): Boolean = false

    /**
     * @return false (not a failure)
     */
    override fun isFailure(): Boolean = false

    /**
     * @return true (this is a reject)
     */
    override fun isRejected(): Boolean = true

    /**
     * @return null (no document updated)
     */
    override fun getOrNull(): T? = null

    /**
     * @return A [ResultExceptionWrapper] containing the rejection exception.
     */
    override fun exceptionOrNull(): ResultExceptionWrapper = ResultExceptionWrapper(
        message = "Update operation rejected.",
        exception = exception,
    )
}
