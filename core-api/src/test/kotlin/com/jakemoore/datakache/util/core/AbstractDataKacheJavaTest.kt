@file:Suppress("unused")

package com.jakemoore.datakache.util.core

import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.util.core.container.MongoDataKacheTestContainer
import com.jakemoore.datakache.util.doc.TestGenericDocCache
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance

/**
 * JUnit 5 base class for Java integration tests against DataKache.
 *
 * Bridges the suspend-based [MongoDataKacheTestContainer] lifecycle via [runBlocking],
 * providing the same real MongoDB environment as [AbstractDataKacheTest] (which targets Kotest).
 *
 * Java test classes should extend this class and annotate test methods with `@Test`.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractDataKacheJavaTest {
    private lateinit var testContainer: MongoDataKacheTestContainer

    @BeforeAll
    fun startContainer() {
        MongoDataKacheTestContainer.startContainers()
        testContainer = MongoDataKacheTestContainer()
        runBlocking { testContainer.beforeSpec() }
    }

    @AfterAll
    fun stopContainer() {
        runBlocking { testContainer.afterSpec() }
        MongoDataKacheTestContainer.stopContainers()
    }

    @BeforeEach
    fun setUpTest() {
        runBlocking { testContainer.beforeEach() }
    }

    @AfterEach
    fun tearDownTest() {
        runBlocking { testContainer.afterEach() }
    }

    val cache: TestGenericDocCache
        get() = testContainer.cache

    val registration: DataKacheRegistration
        get() = testContainer.registration
}
