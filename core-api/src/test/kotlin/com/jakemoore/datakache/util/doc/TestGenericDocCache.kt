@file:Suppress("unused")

package com.jakemoore.datakache.util.doc

import com.jakemoore.datakache.api.cache.GenericDocCache
import com.jakemoore.datakache.api.coroutines.DataKacheScope
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.api.result.Empty
import com.jakemoore.datakache.api.result.OptionalResult
import com.jakemoore.datakache.util.doc.index.BalanceIndex
import com.jakemoore.datakache.util.doc.index.NameIndex
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.KSerializer
import kotlin.reflect.KProperty

class TestGenericDocCache internal constructor(
    registration: DataKacheRegistration,
) : GenericDocCache<TestGenericDoc>(
    registration = registration,
    cacheName = "TestGenericDocs",
    docClass = TestGenericDoc::class.java,
    instantiator = ::TestGenericDoc,
),
    DataKacheScope {
    internal val nameField = NameIndex(this@TestGenericDocCache)
    internal val balanceField = BalanceIndex(this@TestGenericDocCache)

    init {
        // Register indexes
        runBlocking {
            registerUniqueIndex(nameField).exceptionOrNull()?.let {
                throw it
            }
            registerUniqueIndex(balanceField).exceptionOrNull()?.let {
                throw it
            }
        }
    }

    override fun getKSerializer(): KSerializer<TestGenericDoc> = TestGenericDoc.serializer()
    override fun getKeyKProperty(): KProperty<String> = TestGenericDoc::key
    override fun getVersionKProperty(): KProperty<Long> = TestGenericDoc::version

    fun readByName(name: String?): OptionalResult<TestGenericDoc> {
        if (name == null) return Empty()
        return this.readByUniqueIndex(this.nameField, name)
    }

    fun readByBalance(balance: Double?): OptionalResult<TestGenericDoc> {
        if (balance == null) return Empty()
        return this.readByUniqueIndex(this.balanceField, balance)
    }
}
