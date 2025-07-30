@file:Suppress("CanBeParameter", "unused")

package com.jakemoore.datakache.api.result.exception

import java.lang.RuntimeException

class ResultExceptionWrapper(
    // Message is available through Throwable.message, but also via the reason property for convenience
    message: String,
    val exception: Throwable,
    // We pass exception as the 'cause' to maintain the original exception stack trace
    // We also allow direct access as a property on this class for convenience
) : RuntimeException(message, exception) {
    /**
     * The original [com.jakemoore.datakache.api.result.KacheResult] exception message. Includes additional context
     * about the failure, such as the operation that was being performed or the specific error encountered
     */
    val reason: String = message
}
