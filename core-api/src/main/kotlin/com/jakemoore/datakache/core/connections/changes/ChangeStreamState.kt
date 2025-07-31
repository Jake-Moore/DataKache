package com.jakemoore.datakache.core.connections.changes

/**
 * Represents the current state of the change stream connection from the database.
 *
 * Valid state transitions:
 * DISCONNECTED -> CONNECTING -> CONNECTED
 * CONNECTED -> RECONNECTING -> CONNECTED
 * Any state -> FAILED (on error)
 * Any state -> SHUTDOWN (on shutdown)
 */
enum class ChangeStreamState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    RECONNECTING,
    FAILED,
    SHUTDOWN,
}
