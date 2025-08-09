package com.jakemoore.datakache.util.doc.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class MyData(
    val name: String? = null,
    val age: Int = 0,
    @SerialName("myDataList")
    val list: List<String> = emptyList()
) {

    companion object {
        fun createRandom(): MyData {
            return MyData(
                name = randomString(16),
                age = (1..1000).random(),
                list = List(10) { randomString(10) }.shuffled()
            )
        }

        private fun randomString(length: Int = 10): String {
            return (1..length)
                .map { ('a'..'z').random() }
                .joinToString("")
        }
    }
}
