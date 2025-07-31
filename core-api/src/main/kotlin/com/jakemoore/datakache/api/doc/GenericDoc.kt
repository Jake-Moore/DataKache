package com.jakemoore.datakache.api.doc

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.cache.GenericDocCache
import java.util.Objects

/**
 * Represents an IMMUTABLE 'document' in the DataKache system.
 * This is some generic piece of data bound to a given cache by its key [String].
 *
 * CRUD operations are possible on this document,
 * however its state is IMMUTABLE and updates will return a new instance of the document.
 *
 * The cache will always reflect the latest version of the document.
 *
 * Generics:
 * - [D] - The type of the document itself, which must extend [GenericDoc].
 */
abstract class GenericDoc<D : GenericDoc<D>> : Doc<String, D> {
    abstract override val key: String
    abstract override val version: Long

    // ------------------------------------------------------------ //
    //                          API Methods                         //
    // ------------------------------------------------------------ //
    private lateinit var docCache: GenericDocCache<D>
    override fun getDocCache(): DocCache<String, D> {
        return docCache
    }

    // ------------------------------------------------------------ //
    //                      Internal API Methods                    //
    // ------------------------------------------------------------ //
    override fun initializeInternal(cache: DocCache<String, D>) {
        // Allow this function to be called many times
        if (!::docCache.isInitialized || docCache !== cache) {
            require(cache is GenericDocCache<D>) {
                "Cache must be a GenericDocCache"
            }
            docCache = cache
        }
    }

    override fun hashCode(): Int {
        return Objects.hashCode(this.key)
    }

    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        if (other !is GenericDoc<*>) return false
        return this.key == other.key
    }
}
