package com.jakemoore.datakache.plugin.command.sub

import com.jakemoore.datakache.DataKachePlugin
import com.jakemoore.datakache.api.DataKacheAPI
import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.coroutines.DataKacheScope
import com.jakemoore.datakache.api.coroutines.runSync
import com.jakemoore.datakache.api.doc.Doc
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.plugin.command.DataKacheCommand
import com.jakemoore.datakache.plugin.command.api.AbstractCommand
import com.jakemoore.datakache.util.Color
import com.jakemoore.datakache.util.DataKacheFileLogger
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import org.bukkit.Bukkit
import org.bukkit.command.CommandSender

internal class CmdReadDocument(
    parent: DataKacheCommand,
) : AbstractCommand(
    parent = parent,
    commandName = "read-document",
    permission = "datakache.command.read",
    description = "Read A Database Document.",
    argsDescription = "<database> <cache> <document_id>",
),
    DataKacheScope {
    override fun processCommand(sender: CommandSender, args: Array<String>) {
        if (args.size < 3) {
            sendUsage(sender)
            return
        }

        val dbName = args.first()
        val registration = getRegistrationFromDatabase(dbName)
        if (registration == null) {
            sender.sendMessage(Color.t("&cNo database found with the name: &e$dbName"))
            return
        }

        val cacheName = args[1]
        if (cacheName.isBlank()) {
            sender.sendMessage(Color.t("&cCache name cannot be empty."))
            return
        }

        launch {
            processCache(registration, cacheName, sender, args)
        }
    }

    private suspend fun processCache(
        registration: DataKacheRegistration,
        cacheName: String,
        sender: CommandSender,
        args: Array<String>
    ) {
        val docCache: DocCache<*, *>? = getCacheFromRegistration(registration, cacheName)
        if (docCache == null) {
            sender.sendMessage(Color.t("&cNo cache found with the name: &e$cacheName"))
            return
        }

        processReadDocument(args, docCache, sender)
    }

    private suspend fun <K : Any, D : Doc<K, D>> processReadDocument(
        args: Array<String>,
        docCache: DocCache<K, D>,
        sender: CommandSender
    ) {
        val docKeyString = args[2]
        try {
            val key: K = docCache.keyFromString(docKeyString)

            // Read the document from the cache
            val cacheResult = docCache.read(key)
            val cacheException = cacheResult.exceptionOrNull()
            if (cacheException != null) {
                DataKacheFileLogger.severe(
                    msg = "Error reading document from cache '${docCache.cacheName}'",
                    trace = cacheException,
                )
                runSync {
                    sender.sendMessage(
                        Color.t(
                            "&cError reading document from cache: &e${cacheException.message}"
                        )
                    )
                }
                return
            }
            val cacheDoc: D? = cacheResult.getOrNull()

            // Read the document from the database
            val dbResult = docCache.readFromDatabase(key)
            val dbException = dbResult.exceptionOrNull()
            if (dbException != null) {
                DataKacheFileLogger.severe(
                    msg = "Error reading document from database '${docCache.cacheName}'",
                    trace = dbException,
                )
                runSync {
                    sender.sendMessage(
                        Color.t(
                            "&cError reading document from database: &e${dbException.message}"
                        )
                    )
                }
                return
            }
            val dbDoc: D? = dbResult.getOrNull()

            runSync {
                informResults(sender, docCache, key, cacheDoc, dbDoc)
            }
        } catch (_: Exception) {
            sender.sendMessage(Color.t("&cInvalid document key: &e$docKeyString"))
            return
        }
    }

    private fun <K : Any, D : Doc<K, D>> informResults(
        sender: CommandSender,
        docCache: DocCache<K, D>,
        key: K,
        cacheDoc: D?,
        dbDoc: D?
    ) {
        require(Bukkit.isPrimaryThread()) {
            "This method must be called on the main server thread."
        }
        val namespace = docCache.getKeyNamespace(key)

        sender.sendMessage(Color.t("&7--- &9[&bRead Document Result&9]&7 ---"))
        sender.sendMessage(Color.t("&7Cache: &8'&f${docCache.cacheName}&8'"))
        sender.sendMessage(Color.t("&7Key: &8'&f${docCache.keyToString(key)}&8'"))

        if (cacheDoc != null) {
            sender.sendMessage(Color.t("&7Document in Cache: &aFound"))
            sender.sendMessage("&8(&7See console for full document data.&8)")
            val jsonStr = json.encodeToString(docCache.getKSerializer(), cacheDoc)
            requireNotNull(DataKachePlugin.getController()).logger.info(
                "Read Cache Document: ${namespace}\n" + jsonStr
            )
        } else {
            sender.sendMessage(Color.t("&7Document in Cache: &cNot Found"))
        }

        if (dbDoc != null) {
            sender.sendMessage(Color.t("&7Document in Database: &aFound"))
            sender.sendMessage("&8(&7See console for full document data.&8)")
            val jsonStr = json.encodeToString(docCache.getKSerializer(), dbDoc)
            requireNotNull(DataKachePlugin.getController()).logger.info(
                "Read Database Document: ${namespace}\n" + jsonStr
            )
        } else {
            sender.sendMessage(Color.t("&7Document in Database: &cNot Found"))
        }
    }

    override fun processTabComplete(sender: CommandSender, args: Array<String>): List<String> {
        if (args.size <= 1) {
            // Provide tab completion for database names
            return DataKacheAPI.listRegistrations()
                .map { it.databaseName }
                .filter { it.startsWith(args.firstOrNull() ?: "", ignoreCase = true) }
                .sorted()
                .take(20)
        } else if (args.size == 2) {
            // Provide tab completion for cache names in the specified database
            val dbName = args[0]
            val registration = getRegistrationFromDatabase(dbName)
                ?: return emptyList() // No database found, no cache names to complete
            return registration.getDocCaches()
                .map { it.cacheName }
                .filter { it.startsWith(args[1], ignoreCase = true) }
                .sorted()
                .take(20)
        }
        // Otherwise, return empty list for further arguments
        return emptyList()
    }

    private fun getRegistrationFromDatabase(
        databaseName: String,
    ): DataKacheRegistration? {
        return DataKacheAPI.listRegistrations()
            .firstOrNull { it.databaseName == databaseName }
    }

    @Suppress("UNCHECKED_CAST")
    private fun getCacheFromRegistration(
        registration: DataKacheRegistration,
        cacheName: String,
    ): DocCache<*, *>? {
        // Attempt 1 - try an exact name match
        registration.getDocCaches().find {
            it.cacheName.equals(cacheName, ignoreCase = false)
        }?.let { return it }

        // Attempt 2 - try a case-insensitive match
        return registration.getDocCaches().firstOrNull {
            it.cacheName.equals(cacheName, ignoreCase = true)
        }
    }

    companion object {
        private val json = Json {
            encodeDefaults = true
            explicitNulls = true
            prettyPrint = true
        }
    }
}
