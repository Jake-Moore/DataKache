@file:Suppress("unused")

package com.jakemoore.datakache.plugin.command

import com.jakemoore.datakache.util.Color
import org.bukkit.command.CommandSender

/**
 * DataKache subcommand base class. (does not support recursive subcommands)
 */
internal abstract class SubCommand {
    abstract val permission: String?
    abstract val name: String
    abstract val argsDescription: String

    fun sendNoPerm(sender: CommandSender) {
        sender.sendMessage(Color.t(DataKacheCommand.NO_PERM))
    }

    fun sendUsage(sender: CommandSender) {
        sender.sendMessage(
            Color.t(
                "&cNot enough command input. &eYou should use it like this:"
            )
        )
        sender.sendMessage(
            Color.t(
                "&b/${DataKacheCommand.COMMAND_NAME} " + this.name + "&3 " + this.argsDescription
            )
        )
    }

    /**
     * @param args The args of this subcommand (excluding this subcommand name)
     */
    abstract fun execute(sender: CommandSender, args: Array<String>)

    /**
     * @param args The args of this subcommand (excluding this subcommand name)
     */
    open fun getTabCompletions(sender: CommandSender, args: Array<String>): List<String> {
        return listOf()
    }
}
