package com.jakemoore.datakache.util

import kotlin.math.roundToLong

internal class RollingAverage(
    private val size: Int,
) {
    private val readings = ArrayDeque<Long>()
    private var sum: Long = 0L

    fun add(value: Long) {
        readings.addLast(value)
        sum += value

        if (readings.size > size) {
            sum -= readings.removeFirst()
        }
    }

    fun average(): Long {
        return if (readings.isEmpty()) 0L else (sum / readings.size.toDouble()).roundToLong()
    }
}
