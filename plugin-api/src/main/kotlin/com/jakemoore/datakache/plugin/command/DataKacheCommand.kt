package com.jakemoore.datakache.plugin.command

import com.jakemoore.datakache.plugin.command.sub.CmdStatus
import com.jakemoore.datakache.util.Color
import org.bukkit.command.Command
import org.bukkit.command.CommandSender
import org.bukkit.command.TabExecutor

internal class DataKacheCommand : TabExecutor {
    private val subCommands: MutableList<SubCommand> = kotlin.collections.ArrayList()

    init {
        subCommands.add(CmdStatus())
    }

    // Basic command processor to find subcommands and execute them
    override fun onCommand(sender: CommandSender, cmd: Command, label: String, args: Array<String>): Boolean {
        if (!sender.hasPermission("${COMMAND_NAME}.command")) {
            sender.sendMessage(Color.t(NO_PERM))
            return true
        }

        // Attempt to run a subcommand first
        if (args.isNotEmpty()) {
            val subCommandName = args[0]
            val subCommand = subCommands.stream()
                .filter { sub -> sub.name.equals(subCommandName, ignoreCase = true) }
                .findFirst()
                .orElse(null)
            if (subCommand != null) {
                // Check subcommand permission
                if (subCommand.permission != null && !sender.hasPermission(subCommand.permission)) {
                    subCommand.sendNoPerm(sender)
                    return true
                }

                // Provide args (minus the subcommand name)
                if (args.size < 2) {
                    subCommand.execute(sender, arrayOf())
                } else {
                    subCommand.execute(sender, args.copyOfRange(1, args.size))
                }
                return true
            }
        }

        // Collect Viewable SubCommands
        val viewableSubs = mutableListOf<SubCommand>()
        for (sub in subCommands) {
            if (sub.permission == null || sender.hasPermission(sub.permission)) {
                viewableSubs.add(sub)
            }
        }
        if (viewableSubs.isEmpty()) {
            sender.sendMessage(Color.t(NO_PERM))
            return true
        }

        // Send the help menu
        sender.sendMessage(Color.t("&8___ &9[&bHelp for command \"$COMMAND_NAME\"&9] &8___"))
        viewableSubs.forEach { sub: SubCommand ->
            sender.sendMessage(
                Color.t("&b/$COMMAND_NAME " + sub.name + "&3 " + sub.argsDescription)
            )
        }
        return true
    }

    // Basic tab completion processor to find subcommands and provide completions
    override fun onTabComplete(sender: CommandSender, cmd: Command, label: String, args: Array<String>): List<String> {
        val subNames = subCommands
            .map { it.name }
            .toSet()

        // return subcommand names (if no arg stem has started)
        if (args.isEmpty()) {
            return subNames.toList()
        }

        // We have an arg stem, provide completions based on this stem
        if (args.size == 1) {
            val subCommandStem = args[0]
            return subNames
                .filter { subCommand: String ->
                    subCommand.lowercase().startsWith(subCommandStem.lowercase())
                }
                .toList()
        }

        // We have more than 2 arg provided, we should ask for tab completions on that subcommand
        val subCommandName = args[0]
        val subCommand = subCommands.firstOrNull { sub: SubCommand ->
            sub.name.equals(subCommandName, ignoreCase = true)
        } ?: return listOf()

        // We have 2+ args and a subcommand, ask the subcommand for tab completions
        // (array copy is safe, size is >= 2)
        return subCommand.getTabCompletions(sender, args.copyOfRange(1, args.size))
    }

    companion object {
        internal const val NO_PERM = "&cYou do not have permission to use this command."
        internal const val COMMAND_NAME: String = "datakache"
    }
}
