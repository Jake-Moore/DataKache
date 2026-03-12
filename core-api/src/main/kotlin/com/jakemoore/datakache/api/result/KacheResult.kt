package com.jakemoore.datakache.api.result

// Base interface with common functionality
sealed interface KacheResult<T> {
    fun isSuccess(): Boolean

    fun isFailure(): Boolean

    fun getOrNull(): T?

    fun exceptionOrNull(): Throwable?
}

// For operations that CANNOT return Empty
sealed interface DefiniteResult<T> : KacheResult<T> {
    fun getOrThrow(): T
}

// For operations that CAN return Empty
sealed interface OptionalResult<T> : KacheResult<T> {
    fun isEmpty(): Boolean

    /**
     * Returns the value if this result is a [Success], throws the wrapped exception if it is a
     * [Failure], or throws [NoSuchElementException] if it is [Empty].
     *
     * Useful when callers treat an absent document as an exceptional condition.
     */
    fun getOrThrow(): T
}

// For operations that can be rejected
sealed interface RejectableResult<T> : KacheResult<T> {
    fun isRejected(): Boolean
}
