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
     * Informational. A reconnection that may have gone backwards is reported separately, and in
     * stream order, by [onStreamRepositioned].
     */
    suspend fun onConnected()

    /**
     * Called when the stream has reconnected somewhere it may not have reached before, so events
     * after this point can be OLDER than events before it.
     *
     * **Delivered in stream order**, between the last event of the previous connection and the
     * first of the new one, because that is the only moment at which it is true. The producer knows
     * about the reconnection immediately, while the consumer may still be draining events from the
     * connection before it; announcing it directly would apply it to those events too.
     *
     * Not called for the first connection, which begins exactly where its caller asked, nor for a
     * reconnection that resumed from a resume token, which starts immediately after the last event
     * already applied. Only a fallback to an operation time, or to nothing, can go backwards.
     */
    suspend fun onStreamRepositioned()

    /**
     * Called when the change stream disconnects.
     */
    suspend fun onDisconnected()
}
