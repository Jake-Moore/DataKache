package com.jakemoore.datakache.api.config

import com.jakemoore.datakache.api.DataKacheConfig
import com.jakemoore.datakache.api.mode.StorageMode
import com.kamikazejam.kamicommon.configuration.standalone.StandaloneConfig
import com.kamikazejam.kamicommon.util.log.LoggerService
import com.kamikazejam.kamicommon.yaml.base.ConfigurationMethods
import org.bukkit.plugin.java.JavaPlugin
import java.io.File
import java.util.function.Function
import kotlin.time.Duration.Companion.milliseconds

object DataKachePluginConfig {
    const val PLUGIN_CONFIG_FILE = "DataKache.yml"
    // -------------------------------------------------- //
    //                 CONFIGURABLE VALUES                //
    // -------------------------------------------------- //
    /**
     * A supplier that provides the [StandaloneConfig] used to configure the DataKache client.
     * This defaults to trying to find the DataKache.yml file on the filesystem, but can be
     * overridden to provide custom configuration loading logic.
     */
    var configSupplier: Function<JavaPlugin, ConfigurationMethods<*>> = Function { plugin ->
        val file = File(plugin.dataFolder, PLUGIN_CONFIG_FILE)
        // Configure a standalone config that points to the on-disk file for yaml configuration
        StandaloneConfig(DataKachePluginConfigLogger, file) {
            // Use the plugin's embedded resource as the default configuration
            plugin.getResource(PLUGIN_CONFIG_FILE)
        }
    }

    internal fun loadDataKacheConfig(plugin: JavaPlugin): DataKacheConfig {
        val config = getCachedConfig(plugin)
        return DataKacheConfig(
            databaseNamespace = config.getString("database-namespace", "global"),
            debug = config.getBoolean("debug", true),
            storageMode = StorageMode.valueOf(config.getString("storage.mode")),

            // Load MongoDB Connection Details
            mongoURI = config.getString("storage.MONGODB.uri", "mongodb://localhost:27017"),
        )
    }

    internal fun loadDataKacheLang(plugin: JavaPlugin): DataKachePluginLang {
        val config = getCachedConfig(plugin)
        val defaults = DataKachePluginLang()
        return DataKachePluginLang(
            joinDeniedDatabaseNotReady = config.getString("language.joinDenied.databaseNotReady")
                ?: defaults.joinDeniedDatabaseNotReady,
            preloadPlayerDocTimeout = config.getInt("joinOptions.preloadPlayerDocTimeoutMS", -1)
                .takeIf { it > 0 }?.milliseconds ?: defaults.preloadPlayerDocTimeout,
            joinDeniedPlayerDocTimeout = config.getString("language.joinDenied.playerDocTimeout")
                ?: defaults.joinDeniedPlayerDocTimeout,
            joinDeniedPlayerDocException = config.getString("language.joinDenied.playerDocException")
                ?: defaults.joinDeniedPlayerDocException,
            joinDeniedEarlyJoin = config.getString("language.joinDenied.earlyJoin")
                ?: defaults.joinDeniedEarlyJoin,
        )
    }

    // Store one lateinit StandaloneConfig instance for reuse
    private lateinit var cachedConfig: ConfigurationMethods<*>

    private fun getCachedConfig(plugin: JavaPlugin): ConfigurationMethods<*> {
        if (!this::cachedConfig.isInitialized) {
            cachedConfig = configSupplier.apply(plugin)
        }
        return cachedConfig
    }

    object DataKachePluginConfigLogger : LoggerService() {
        override fun getLoggerName(): String {
            return "DataKachePluginConfigLogger"
        }

        override fun isDebug(): Boolean {
            return true
        }
    }
}
