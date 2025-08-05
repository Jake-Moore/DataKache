package com.jakemoore.datakache.plugin.command.sub

import com.jakemoore.datakache.api.coroutines.DataKacheScope
import com.jakemoore.datakache.plugin.command.DataKacheCommand
import com.jakemoore.datakache.plugin.command.api.AbstractCommand
import com.jakemoore.datakache.util.Color
import org.bukkit.command.CommandSender

internal class CmdCoroutines(
    parent: DataKacheCommand,
) : AbstractCommand(
    parent = parent,
    commandName = "coroutines",
    permission = "datakache.command.coroutines",
    argsDescription = "",
    description = "View Running Coroutines.",
) {

    override fun processCommand(sender: CommandSender, args: Array<String>) {
        sender.sendMessage(Color.t("&7--- &9[&bDataKache Coroutines&9]&7 ---"))
        sender.sendMessage(Color.t("&7Coroutine Count: &8'&f${DataKacheScope.runningCoroutinesCount()}&8'"))
    }
}
