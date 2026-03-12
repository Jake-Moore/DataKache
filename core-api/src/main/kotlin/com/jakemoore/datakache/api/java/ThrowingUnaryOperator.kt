package com.jakemoore.datakache.api.java

/**
 * A functional interface equivalent to [java.util.function.UnaryOperator] that declares
 * `throws Exception`, allowing Java lambdas to throw checked exceptions such as
 * [com.jakemoore.datakache.api.exception.update.RejectUpdateException].
 *
 * Used as the update function parameter in rejectable async operations
 * (e.g., [com.jakemoore.datakache.api.cache.DocCache.updateRejectableAsync]).
 */
fun interface ThrowingUnaryOperator<T> {
    @Throws(Exception::class)
    fun apply(t: T): T
}
