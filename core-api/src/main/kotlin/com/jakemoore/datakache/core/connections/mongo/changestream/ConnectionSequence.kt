package com.jakemoore.datakache.core.connections.mongo.changestream

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Whether a change stream has connected before, which is what separates its first connection from a
 * reconnection.
 *
 * Deliberately not derived from the stream's state machine. That machine forces the state back to
 * connecting at the top of every retry attempt, so by the time a connection succeeds it no longer
 * records that the attempt was a retry, and a check written against it can never fire.
 */
internal class ConnectionSequence {
    private val connected = AtomicBoolean(false)

    /**
     * Records a successful connection.
     *
     * @return False for the first one, true for every later one.
     */
    fun observeConnection(): Boolean = !connected.compareAndSet(false, true)
}
