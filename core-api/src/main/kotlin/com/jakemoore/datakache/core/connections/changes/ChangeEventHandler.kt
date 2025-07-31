package com.jakemoore.datakache.core.connections.changes

import com.jakemoore.datakache.api.doc.Doc

/**
 * Handles change event processing for the cache.
 */
interface ChangeEventHandler<K : Any, D : Doc<K, D>> {
    /**
     * Called when a document is inserted, updated, or replaced.
     * @param doc The full [Doc] after the change.
     * @param operationType The type of operation (INSERT, UPDATE, REPLACE).
     */
    suspend fun onDocumentChanged(doc: D, operationType: ChangeOperationType)

    /**
     * Called when a document is deleted.
     * @param key The key of the deleted [Doc].
     */
    suspend fun onDocumentDeleted(key: K)

    /**
     * Called when the change stream connects successfully.
     */
    suspend fun onConnected()

    /**
     * Called when the change stream disconnects.
     */
    suspend fun onDisconnected()
}
