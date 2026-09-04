package com.jakemoore.datakache.core.connections.changes

import com.jakemoore.datakache.api.changes.ChangeDocumentType
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.ordering.OperationTime
import org.jetbrains.annotations.ApiStatus

/**
 * Handles change event processing for the cache.
 */
@ApiStatus.Internal
interface ChangeEventHandler<K : Any, D : Doc<K, D>> {
    /**
     * Called when a document is inserted, updated, or replaced.
     * @param doc The full [Doc] after the change.
     * @param changeType The change operation which produced the document.
     * @param at The position of this event in the database's ordering.
     * @param outOfBand See [onDocumentDeleted].
     */
    suspend fun onDocumentChanged(doc: D, changeType: ChangeDocumentType, at: OperationTime, outOfBand: Boolean)

    /**
     * Called when a document is deleted.
     *
     * @param keyString The String representation of the key of the deleted [Doc].
     * @param at The position of this event in the database's ordering.
     * @param outOfBand True when this event skipped the ordered buffer and was applied immediately,
     * which the implementation does when that buffer is saturated and dropping the event would be
     * worse. Such an event can therefore be applied AHEAD of events that are still queued and older
     * than it, so a handler must not treat it as evidence that everything up to [at] has been
     * applied. Ordinary events, delivered in the database's own commit order, do carry that meaning.
     */
    suspend fun onDocumentDeleted(keyString: String, at: OperationTime, outOfBand: Boolean)

    /**
     * Called when the collection is dropped.
     */
    suspend fun onCollectionDropped()

    /**
     * Called when the collection is renamed.
     */
    suspend fun onCollectionRenamed()

    /**
     * Called when the database is dropped.
     */
    suspend fun onDatabaseDropped()

    /**
     * Called when the change stream is invalidated.
     */
    suspend fun onChangeStreamInvalidated()

    /**
     * Called when an unknown operation type is encountered.
     */
    suspend fun onUnknownOperation()

    /**
     * Called when the change stream connects successfully.
     *
     * @param mayHaveRepositioned True when this connection could be reading from a point EARLIER
     * than the stream had already reached, so anything a handler concluded from the previous
     * connection's progress no longer holds.
     *
     * False for the first connection, which begins exactly where its caller asked, and false for a
     * reconnection that resumed from a resume token, which starts immediately after the last event
     * already applied. **Only a reconnection that fell back to a time, or to nothing, can go
     * backwards**, and treating every reconnection as if it had would be safe but expensive:
     * reconnections are routine, and a handler that discards its progress on each one never gets to
     * use it.
     */
    suspend fun onConnected(mayHaveRepositioned: Boolean)

    /**
     * Called when the change stream disconnects.
     */
    suspend fun onDisconnected()
}
