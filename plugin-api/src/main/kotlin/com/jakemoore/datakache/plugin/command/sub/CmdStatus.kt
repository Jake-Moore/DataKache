package com.jakemoore.datakache.plugin.command.sub

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.plugin.command.DataKacheCommand
import com.jakemoore.datakache.plugin.command.api.AbstractCommand
import com.jakemoore.datakache.util.Color
import org.bukkit.command.CommandSender

internal class CmdStatus(
    parent: DataKacheCommand,
) : AbstractCommand(
    parent = parent,
    commandName = "status",
    permission = "datakache.command.status",
    argsDescription = "",
    description = "View DataKache Status.",
) {

    override fun processCommand(sender: CommandSender, args: Array<String>) {
        sender.sendMessage(Color.t("&7--- &9[&bDataKache Status&9]&7 ---"))
        sender.sendMessage(Color.t("&7Database namespace: &8'&f${DataKache.databaseNamespace}&8'"))
        val storageStatus = if (DataKache.storageMode.isDatabaseReadyForWrites()) {
            "&aReady"
        } else {
            "&cNot Ready"
        }
        sender.sendMessage(Color.t("&7Storage Mode: &f${DataKache.storageMode.name} &7($storageStatus&7)"))

        val averageMS = DataKache.storageMode.getDatabaseServiceAveragePing() / 1_000_000
        sender.sendMessage(
            Color.t(
                "&7Storage Pings &8(&7average &f${averageMS}ms&8)"
            )
        )
        for ((address, pingNS) in DataKache.storageMode.getDatabaseServiceServerPings()) {
            sender.sendMessage(
                Color.t(
                    "  &8'&f${trimAddress(address)}&8' &7-> &f${pingNS / 1_000_000}ms &7(${pingNS}ns)"
                )
            )
        }
    }

    /**
     * Splices out the middle of an address. The host portion has only its first and last 5 characters.
     */
    private fun trimAddress(address: String): String {
        val parts = address.split(":")
        if (parts.isEmpty()) {
            return address // Invalid address, return as is
        }

        val host = parts.first()
        val port: String? = if (parts.size > 1) parts[1] else null

        return if (host.length <= 10) {
            host + (port?.let { ":$it" } ?: "")
        } else {
            host.substring(0, 5) + "..." + host.substring(host.length - 5) + (port?.let { ":$it" } ?: "")
        }
    }
}
