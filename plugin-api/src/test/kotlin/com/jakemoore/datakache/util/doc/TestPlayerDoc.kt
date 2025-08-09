package com.jakemoore.datakache.util.doc

import com.jakemoore.datakache.api.doc.PlayerDoc
import com.jakemoore.datakache.api.serialization.java.UUIDSerializer
import com.jakemoore.datakache.util.doc.data.MyData
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.util.UUID

@Serializable
data class TestPlayerDoc(
    // Parent Properties
    @Serializable(with = UUIDSerializer::class)
    @SerialName("_id") override val key: UUID,
    override val version: Long,
    override val username: String?,
    // TestPlayerDoc Properties
    val name: String? = null,
    val balance: Double = 0.0,
    @SerialName("myList")
    val list: List<String> = emptyList(),
    val customList: List<MyData> = emptyList(),
    val customSet: Set<MyData> = emptySet(),
    val customMap: Map<String, MyData> = emptyMap(),
) : PlayerDoc<TestPlayerDoc>() {

    override fun copyHelper(version: Long): TestPlayerDoc {
        return this.copy(version = version)
    }

    override fun copyHelper(username: String?): TestPlayerDoc {
        return this.copy(username = username)
    }
}
