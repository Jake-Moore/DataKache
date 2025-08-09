package com.jakemoore.datakache.api.result

import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper

/**
 * Failure state (exception during operation).
 */
class Failure<T>(
    val exception: ResultExceptionWrapper,
) : DefiniteResult<T>, OptionalResult<T>, RejectableResult<T> {
    /**
     * @return false (not a success)
     */
    override fun isSuccess(): Boolean = false

    /**
     * @return true (this is a failure)
     */
    override fun isFailure(): Boolean = true

    /**
     * @return false (not empty)
     */
    override fun isEmpty(): Boolean = false

    /**
     * @return false (reject is a separate state)
     */
    override fun isRejected(): Boolean = false

    /**
     * @return null (no document due to failure)
     */
    override fun getOrNull(): T? = null

    /**
     * @return the exception ([exception]) that caused this failure
     */
    override fun exceptionOrNull(): Throwable = exception

    /**
     * Throws the exception wrapped in this failure.
     */
    override fun getOrThrow(): T = throw exception
}
