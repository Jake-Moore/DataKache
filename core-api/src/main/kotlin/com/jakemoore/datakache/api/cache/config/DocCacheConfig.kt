package com.jakemoore.datakache.api.cache.config

import com.jakemoore.datakache.api.doc.Doc

data class DocCacheConfig<K : Any, D : Doc<K, D>>(
    /**
     * If true, a replayed change stream update whose version already matches the cached document is
     * not written into the cache again, on the assumption that an equal version means equal data.
     *
     * **Narrower than it sounds, and deliberately so.** It applies to replayed UPDATE events only.
     * Local writes never take the shortcut, because they are authoritative on their own content
     * whether or not the version moved, and REPLACE and INSERT do not either, because neither
     * guarantees the version differs when the content does. Reads ignore it entirely. See
     * [com.jakemoore.datakache.api.cache.DocCache.cacheInternal] for why each of those holds.
     *
     * Turning it off costs a map write per replayed update and changes no observable behaviour.
     */
    val optimisticCaching: Boolean,
    /**
     * If true, the cache will allow mass-destructive operations such as:
     * - [com.jakemoore.datakache.api.cache.DocCache.clearDocsFromDatabasePermanently]
     *
     * WARNING: Irreversible. Intended for tests or tightly controlled admin tooling only.
     */
    val enableMassDestructiveOps: Boolean,
) {
    companion object {
        fun <K : Any, D : Doc<K, D>> default(): DocCacheConfig<K, D> =
            DocCacheConfig(
            optimisticCaching = true,
            enableMassDestructiveOps = false,
        )
    }
}
