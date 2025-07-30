package com.jakemoore.datakache.api.result

/**
 * Empty state (no data, no exception).
 */
class Empty<T> : OptionalResult<T> {
    /**
     * @return false (not a success)
     */
    override fun isSuccess(): Boolean = false

    /**
     * @return false (not a failure)
     */
    override fun isFailure(): Boolean = false

    /**
     * @return true (this is an empty result)
     */
    override fun isEmpty(): Boolean = true

    /**
     * @return null (no data)
     */
    override fun getOrNull(): T? = null

    /**
     * @return null (no exception)
     */
    override fun exceptionOrNull(): Throwable? = null
}
