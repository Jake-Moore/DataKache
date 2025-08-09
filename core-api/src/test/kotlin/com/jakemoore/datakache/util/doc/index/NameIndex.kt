package com.jakemoore.datakache.util.doc.index

import com.jakemoore.datakache.api.index.DocUniqueIndex
import com.jakemoore.datakache.util.doc.TestGenericDoc
import com.jakemoore.datakache.util.doc.TestGenericDocCache

class NameIndex(
    cache: TestGenericDocCache
) : DocUniqueIndex<String, TestGenericDoc, String>(
    docCache = cache,
    kProperty = TestGenericDoc::name,
) {
    override fun equals(a: String?, b: String?): Boolean {
        if (a == null || b == null) {
            return a == null && b == null
        }
        // This specific implementation in the context of a "player name" wants to ignore case
        // Not all implementations will want to do this, but it is possible since we have a custom equals method
        return a.equals(b, ignoreCase = true)
    }

    override fun extractValue(doc: TestGenericDoc): String? {
        return doc.name
    }
}
