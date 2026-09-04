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
     * @param reconnected False on the first connection of this stream, which begins exactly where
     * its caller asked it to. True on every later one, where the stream may resume from a point
     * EARLIER than it had already reached and replay history, so anything a handler concluded from
     * the previous connection's progress no longer holds.
     */
    suspend fun onConnected(reconnected: Boolean)

    /**
     * Called when the change stream disconnects.
     */
    suspend fun onDisconnected()
}
