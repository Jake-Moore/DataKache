package com.jakemoore.datakache.api.cache.config

import com.jakemoore.datakache.api.doc.Doc

data class DocCacheConfig<K : Any, D : Doc<K, D>>(
    /**
     * If true, documents that are internally being added to the cache will follow optimistic versioning rules.
     *
     * This means that if the document in cache shares the same version,
     * the new document will not be inserted in its place. (i.e. we assume that an equal version means equal data)
     */
    val optimisticCaching: Boolean,

    /**
     * If true, the cache will allow mass-destructive operations such as:
     * - [com.jakemoore.datakache.api.cache.DocCache.clearDocsFromDatabasePermanently]
     */
    val enableMassDestructiveOps: Boolean,
) {
    companion object {
        fun <K : Any, D : Doc<K, D>> default(): DocCacheConfig<K, D> {
            return DocCacheConfig(
                optimisticCaching = true,
                enableMassDestructiveOps = false,
            )
        }
    }
}
