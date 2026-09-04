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
     */
    suspend fun onDocumentChanged(doc: D, changeType: ChangeDocumentType, at: OperationTime)

    /**
     * Called when a document is deleted.
     * @param keyString The String representation of the key of the deleted [Doc].
     */
    suspend fun onDocumentDeleted(keyString: String, at: OperationTime)

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
     */
    suspend fun onConnected()

    /**
     * Called when the change stream disconnects.
     */
    suspend fun onDisconnected()
}
