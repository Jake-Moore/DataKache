package com.jakemoore.datakache.test.config

import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.spec.SpecExecutionOrder

/**
 * Kotest project configuration for DataKache tests
 *
 * This configuration sets up:
 * - Spec execution order to use annotations for ordering
 */
@Suppress("unused")
object KotestConfig : AbstractProjectConfig() {

    override val specExecutionOrder = SpecExecutionOrder.Annotated
}
