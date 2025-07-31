package com.jakemoore.datakache.core.connections.changes

/**
 * Represents the current state of the change stream connection from the database.
 */
enum class ChangeStreamState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    RECONNECTING,
    FAILED,
    SHUTDOWN,
}
