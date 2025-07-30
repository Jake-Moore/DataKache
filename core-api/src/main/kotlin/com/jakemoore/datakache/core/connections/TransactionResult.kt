package com.jakemoore.datakache.core.connections

import com.jakemoore.datakache.api.doc.Doc

/**
 * Internal class representing the result of a transaction operation on a [Doc]
 *
 * [doc] represents the successfully processed document. (nonnull = success).
 * [databaseDoc] represents the database document that was fetched (nonnull = failure requiring retry).
 */
internal class TransactionResult<K : Any, D : Doc<K, D>>(
    val doc: D?,
    val databaseDoc: D? = null,
)
