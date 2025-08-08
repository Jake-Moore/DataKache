package com.jakemoore.datakache.util.doc.index

import com.jakemoore.datakache.api.index.DocUniqueIndex
import com.jakemoore.datakache.util.doc.TestGenericDoc
import com.jakemoore.datakache.util.doc.TestGenericDocCache

class BalanceIndex(
    cache: TestGenericDocCache
) : DocUniqueIndex<String, TestGenericDoc, Double>(
    docCache = cache,
    kProperty = TestGenericDoc::balance,
) {
    override fun equals(a: Double?, b: Double?): Boolean {
        if (a == null || b == null) {
            return a == null && b == null
        }
        return a == b
    }

    override fun extractValue(doc: TestGenericDoc): Double {
        return doc.balance
    }
}
