@file:Suppress("unused")

package com.jakemoore.datakache.util.doc

import com.jakemoore.datakache.api.cache.PlayerDocCache
import com.jakemoore.datakache.api.coroutines.DataKacheScope
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import kotlinx.serialization.KSerializer
import org.bukkit.plugin.java.JavaPlugin
import java.util.UUID
import kotlin.reflect.KProperty

class TestPlayerDocCache internal constructor(
    plugin: JavaPlugin,
    registration: DataKacheRegistration,
) : PlayerDocCache<TestPlayerDoc>(
    plugin = plugin,
    registration = registration,
    cacheName = "TestPlayerDocs",
    docClass = TestPlayerDoc::class.java,
    instantiator = ::TestPlayerDoc,

    defaultInitializer = { it },
),
    DataKacheScope {

    override fun getKSerializer(): KSerializer<TestPlayerDoc> = TestPlayerDoc.serializer()
    override fun getKeyKProperty(): KProperty<UUID> = TestPlayerDoc::key
    override fun getVersionKProperty(): KProperty<Long> = TestPlayerDoc::version
    override fun getUsernameKProperty(): KProperty<String?> = TestPlayerDoc::username
}
