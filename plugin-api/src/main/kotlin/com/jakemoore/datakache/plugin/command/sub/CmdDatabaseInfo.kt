package com.jakemoore.datakache.plugin.command.sub

import com.jakemoore.datakache.api.DataKacheAPI
import com.jakemoore.datakache.api.registration.DataKacheRegistration
import com.jakemoore.datakache.plugin.command.DataKacheCommand
import com.jakemoore.datakache.plugin.command.api.AbstractCommand
import com.jakemoore.datakache.util.Color
import org.bukkit.command.CommandSender

internal class CmdDatabaseInfo(
    parent: DataKacheCommand,
) : AbstractCommand(
    parent = parent,
    commandName = "database-info",
    permission = "datakache.command.database.info",
    description = "View Database Information.",
    argsDescription = "<database_name>",
) {
    override fun processCommand(sender: CommandSender, args: Array<String>) {
        if (args.isEmpty()) {
            sendUsage(sender)
            return
        }

        val dbName = args.first()
        val registration = getRegistrationFromDatabase(dbName)
        if (registration == null) {
            sender.sendMessage(Color.t("&cNo database found with the name: &e$dbName"))
            return
        }

        sender.sendMessage(Color.t("&7--- &9[&bDatabase Information&9]&7 ---"))
        sender.sendMessage(Color.t("&7Database: &8'&f${registration.databaseName}&8'"))
        sender.sendMessage(Color.t("&7Caches:"))
        for (docCache in registration.getDocCaches()) {
            sender.sendMessage(
                Color.t(
                    "&7 - &8'&f${docCache.cacheName}&8' &7(${docCache.getCacheSize()} objects)"
                )
            )
        }
    }

    override fun processTabComplete(sender: CommandSender, args: Array<String>): List<String> {
        // Provide tab completion for database names
        if (args.size > 1) {
            // no tab completions for 2nd+ arguments
            return emptyList()
        }

        val stem: String? = if (args.isEmpty()) null else args.first()
        return DataKacheAPI.listRegistrations()
            .map { it.databaseName }
            .filter { it.startsWith(stem ?: "", ignoreCase = true) }
            .sorted()
            .take(20)
    }

    private fun getRegistrationFromDatabase(databaseName: String): DataKacheRegistration? {
        return DataKacheAPI.listRegistrations()
            .firstOrNull { it.databaseName == databaseName }
    }
}
