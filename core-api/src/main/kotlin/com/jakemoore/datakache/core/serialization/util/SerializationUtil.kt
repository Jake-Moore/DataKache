package com.jakemoore.datakache.core.serialization.util

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.doc.Doc
import kotlinx.serialization.SerialName
import kotlin.reflect.KProperty
import kotlin.reflect.full.findAnnotation

@Suppress("unused")
internal object SerializationUtil {
    fun getSerialNameFromProperty(property: KProperty<*>): String {
        return property.findAnnotation<SerialName>()?.value ?: property.name
    }

    fun <K : Any, D : Doc<K, D>> getSerialNameForKey(docCache: DocCache<K, D>): String {
        return getSerialNameFromProperty(docCache.getKeyKProperty())
    }

    fun <K : Any, D : Doc<K, D>> getSerialNameForVersion(docCache: DocCache<K, D>): String {
        return getSerialNameFromProperty(docCache.getVersionKProperty())
    }
}
