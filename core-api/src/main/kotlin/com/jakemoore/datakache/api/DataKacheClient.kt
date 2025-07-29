package com.jakemoore.datakache.api

/**
 * A single "client" of DataKache (plugin, service, application, module, etc.).
 *
 * Separate clients should be used for unique applications living in the same classpath or environment.
 */
interface DataKacheClient {
    /** A unique client identifier for metrics and logging. */
    val name: String
}
