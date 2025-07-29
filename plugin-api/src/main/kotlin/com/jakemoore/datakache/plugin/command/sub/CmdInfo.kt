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
        sender.sendMessage(Color.t("&7Database Prefix:"))
        sender.sendMessage(Color.t("  &8'&f${DataKache.databasePrefix}&8'"))
        sender.sendMessage(Color.t("&7Storage Mode: &f${DataKache.storageMode.name}"))
    }
}
