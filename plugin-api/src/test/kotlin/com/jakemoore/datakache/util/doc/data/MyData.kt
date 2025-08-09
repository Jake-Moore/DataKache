@file:Suppress("unused")

package com.jakemoore.datakache.util.doc.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

// Random data class with 1 string, 1 int, 1 BigInteger, and one inner list
@Suppress("PROVIDED_RUNTIME_TOO_LOW")
@Serializable
data class MyData(
    val name: String? = null,
    val age: Int = 0,
    @SerialName("myDataList")
    val list: List<String> = emptyList()
) {

    companion object {
        private const val SAMPLE_NAME = "John"
        private const val SAMPLE_AGE = 25
        private val SAMPLE_LIST: List<String> = listOf("a", "b", "c")

        fun createSample(): MyData {
            return MyData(
                SAMPLE_NAME,
                SAMPLE_AGE,
                SAMPLE_LIST
            )
        }

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
