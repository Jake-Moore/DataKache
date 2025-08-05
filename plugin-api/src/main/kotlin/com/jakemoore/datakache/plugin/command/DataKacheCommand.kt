package com.jakemoore.datakache.plugin.command

import com.jakemoore.datakache.plugin.command.api.AbstractCommand
import com.jakemoore.datakache.plugin.command.sub.CmdCoroutines
import com.jakemoore.datakache.plugin.command.sub.CmdDatabaseInfo
import com.jakemoore.datakache.plugin.command.sub.CmdListDatabases
import com.jakemoore.datakache.plugin.command.sub.CmdStatus
import org.bukkit.command.Command
import org.bukkit.command.CommandSender
import org.bukkit.command.TabExecutor

internal class DataKacheCommand :
    AbstractCommand(
        parent = null,
        commandName = "datakache",
        permission = "datakache.command",
    ),
    TabExecutor {

    init {
        subCommands.add(CmdStatus(this))
        subCommands.add(CmdListDatabases(this))
        subCommands.add(CmdCoroutines(this))
        subCommands.add(CmdDatabaseInfo(this))
    }

    override fun onCommand(sender: CommandSender, cmd: Command, label: String, args: Array<String>): Boolean {
        super.processCommand(sender, args)
        return true
    }

    override fun onTabComplete(sender: CommandSender, cmd: Command, label: String, args: Array<String>): List<String> {
        return super.processTabComplete(sender, args)
    }
}
