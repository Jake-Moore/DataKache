package com.jakemoore.datakache.api.result

/**
 * Success state ([value] is available, no exceptions or rejections occurred).
 */
class Success<T>(
    val value: T
) : DefiniteResult<T>, OptionalResult<T>, RejectableResult<T> {
    /**
     * @return true (this is a success state).
     */
    override fun isSuccess(): Boolean = true

    /**
     * @return false (this is not a failure).
     */
    override fun isFailure(): Boolean = false

    /**
     * @return false (this is not an empty state).
     */
    override fun isEmpty(): Boolean = false

    /**
     * @return false (this is not a rejected state).
     */
    override fun isRejected(): Boolean = false

    /**
     * @return the [value] that was successfully retrieved.
     */
    override fun getOrNull(): T = value

    /**
     * @return null (no exception occurred).
     */
    override fun exceptionOrNull(): Throwable? = null
}
