package com.jakemoore.datakache.plugin.command.sub

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.api.DataKacheAPI
import com.jakemoore.datakache.plugin.command.DataKacheCommand
import com.jakemoore.datakache.plugin.command.api.AbstractCommand
import com.jakemoore.datakache.util.Color
import org.bukkit.command.CommandSender

internal class CmdListDatabases(
    parent: DataKacheCommand,
) : AbstractCommand(
    parent = parent,
    commandName = "list-databases",
    permission = "datakache.command.list.databases",
    argsDescription = "",
    description = "View Registered Databases.",
) {

    override fun processCommand(sender: CommandSender, args: Array<String>) {
        val storage = DataKache.storageMode
        val readyState = if (storage.isDatabaseReadyForWrites()) "&aConnected" else "&cDisconnected"
        val dbNamespace = DataKache.databaseNamespace

        sender.sendMessage(Color.t("&7--- &9[&bDataKache Database Info&9]&7 ---"))
        sender.sendMessage(Color.t("&7Database namespace: &8'&f$dbNamespace&8'"))
        sender.sendMessage(Color.t("&7Storage Service: $readyState"))

        sender.sendMessage(Color.t("&7Databases:"))
        DataKacheAPI.listDatabases()
            .map { registration -> registration.databaseName }
            .forEach { dbName -> sender.sendMessage(Color.t("&7 - &f$dbName")) }
    }
}
