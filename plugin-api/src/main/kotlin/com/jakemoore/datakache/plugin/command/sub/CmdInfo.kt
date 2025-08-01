package com.jakemoore.datakache.plugin.command.sub

import com.jakemoore.datakache.DataKache
import com.jakemoore.datakache.plugin.command.DataKacheCommand
import com.jakemoore.datakache.plugin.command.SubCommand
import com.jakemoore.datakache.util.Color
import org.bukkit.command.CommandSender

internal class CmdInfo : SubCommand() {
    override val name: String
        get() = "info"

    override val permission: String
        get() = "${DataKacheCommand.Companion.COMMAND_NAME}.command.info"

    override val argsDescription: String
        get() = ""

    override fun execute(sender: CommandSender, args: Array<String>) {
        sender.sendMessage(Color.t("&7--- &9[&bDataKache Information&9]&7 ---"))
        sender.sendMessage(Color.t("&7Database namespace: &8'&f${DataKache.databaseNamespace}&8'"))
        sender.sendMessage(Color.t("&7Storage Mode: &f${DataKache.storageMode.name}"))

        sender.sendMessage(
            Color.t(
                "&7Storage Pings &8(&7average &f${DataKache.storageMode.getDatabaseServiceAveragePing()}ms&8)"
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
     * Splices out the middle of an address, keeping only the host portion with its first and last 5 characters.
     */
    private fun trimAddress(address: String): String {
        val host = address.split(":").first()
        return if (host.length <= 10) {
            host
        } else {
            host.substring(0, 5) + "..." + host.substring(host.length - 5)
        }
    }
}
