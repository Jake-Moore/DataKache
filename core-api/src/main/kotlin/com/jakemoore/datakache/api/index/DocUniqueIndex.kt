package com.jakemoore.datakache.api.index

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException
import com.jakemoore.datakache.api.exception.DuplicateUniqueIndexException
import com.jakemoore.datakache.core.serialization.util.SerializationUtil
import kotlin.reflect.KProperty

/**
 * Wrapper object to represent a unique index on a DocCache.
 *
 * This index will have uniqueness enforced, just like a primary key.
 * This enforcement is handled by the backing database or storage system.
 *
 * When using indexes, additional exceptions are possible for CRUD operations on documents.
 *
 * The Result wrapper for [DocCache.create] and [DocCache.update] may contain the following exceptions:
 * - [DuplicateDocumentKeyException] if the primary key already exists.
 * - [DuplicateUniqueIndexException] if a unique index is violated.
 */
abstract class DocUniqueIndex<K : Any, D : Doc<K, D>, T>(
    internal val docCache: DocCache<K, D>,
    private val kProperty: KProperty<T?>,
) {
    val fieldName: String
        get() = SerializationUtil.getSerialNameFromProperty(kProperty)

    /**
     * Checks if the two values from the indexes of two documents are equal.
     */
    abstract fun equals(a: T?, b: T?): Boolean

    /**
     * Extracts the value from the document for this index.
     */
    abstract fun extractValue(doc: D): T?
}
