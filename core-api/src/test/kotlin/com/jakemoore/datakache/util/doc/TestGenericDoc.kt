package com.jakemoore.datakache.util.doc

import com.jakemoore.datakache.api.doc.GenericDoc
import com.jakemoore.datakache.util.doc.data.MyData
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Suppress("PROVIDED_RUNTIME_TOO_LOW", "MemberVisibilityCanBePrivate")
@Serializable
data class TestGenericDoc(
    // Parent Properties
    @SerialName("_id") override val key: String,
    override val version: Long,
    // RandomGenericDoc Properties
    val name: String? = null,
    val balance: Double = 0.0,
    @SerialName("myList")
    val list: List<String> = emptyList(),
    val customList: List<MyData> = emptyList(),
    val customSet: Set<MyData> = emptySet(),
    val customMap: Map<String, MyData> = emptyMap(),
) : GenericDoc<TestGenericDoc>() {

    override fun copyHelper(version: Long): TestGenericDoc {
        return this.copy(version = version)
    }
}
