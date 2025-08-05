package com.jakemoore.datakache.plugin.command.api

import com.jakemoore.datakache.util.Color
import org.bukkit.command.CommandSender

@Suppress("unused")
internal abstract class AbstractCommand(
    protected val commandName: String,
    protected val permission: String,
    protected val parent: AbstractCommand?,
    /**
     * Optional Descriptor string for the arguments of this command.
     */
    protected val argsDescription: String? = null,
    /**
     * Optional Description of this command.
     */
    protected val description: String? = null
) {
    protected val subCommands: MutableList<AbstractCommand> = kotlin.collections.ArrayList()

    protected fun sendUsage(sender: CommandSender) {
        sender.sendMessage(
            Color.t(
                "&4Not enough command input. &cYou should use it like this:"
            )
        )
        sender.sendMessage(getCommandUsage(this))
    }

    protected fun getCommandUsage(): String {
        return getCommandUsage(this)
    }

    protected fun sendNoPerm(sender: CommandSender) {
        sender.sendMessage(Color.t(NO_PERM))
    }

    // Basic command processor to find subcommands and execute them
    @Suppress("unused")
    protected open fun processCommand(sender: CommandSender, args: Array<String>) {
        if (!sender.hasPermission(permission)) {
            sender.sendMessage(Color.t(NO_PERM))
            return
        }

        // Attempt to run a subcommand first
        if (args.isNotEmpty()) {
            val subCommandName = args[0]
            val subCommand = subCommands.stream()
                .filter { sub -> sub.commandName.equals(subCommandName, ignoreCase = true) }
                .findFirst()
                .orElse(null)
            if (subCommand != null) {
                // Check subcommand permission
                if (!sender.hasPermission(subCommand.permission)) {
                    subCommand.sendNoPerm(sender)
                    return
                }

                // Provide args (minus the subcommand name)
                if (args.size < 2) {
                    subCommand.processCommand(sender, arrayOf())
                } else {
                    subCommand.processCommand(sender, args.copyOfRange(1, args.size))
                }
                return
            }
        }

        // Collect Viewable SubCommands
        val viewableSubs = mutableListOf<AbstractCommand>()
        for (sub in subCommands) {
            if (sender.hasPermission(sub.permission)) {
                viewableSubs.add(sub)
            }
        }
        if (viewableSubs.isEmpty()) {
            sender.sendMessage(Color.t(NO_PERM))
            return
        }

        // Send the help menu
        sender.sendMessage(Color.t("&8___ &9[&bHelp for command \"$commandName\"&9] &8___"))
        viewableSubs.forEach { sub ->
            sender.sendMessage(sub.getCommandUsage())
        }
    }

    // Basic tab completion processor to find subcommands and provide completions
    @Suppress("unused")
    protected open fun processTabComplete(sender: CommandSender, args: Array<String>): List<String> {
        val subNames = subCommands
            .map { it.commandName }
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
        val subCommand = subCommands.firstOrNull { sub ->
            sub.commandName.equals(subCommandName, ignoreCase = true)
        } ?: return listOf()

        // We have 2+ args and a subcommand, ask the subcommand for tab completions
        // (array copy is safe, size is >= 2)
        return subCommand.processTabComplete(sender, args.copyOfRange(1, args.size))
    }

    companion object {
        internal const val NO_PERM = "&cYou do not have permission to use this command."

        fun getCommandUsage(command: AbstractCommand): String {
            // We need to compile the command stems from each of the parent commands
            val commandStems = mutableListOf<String>()
            var current: AbstractCommand? = command
            while (current != null) {
                commandStems.add(current.commandName)
                current = current.parent
            }
            commandStems.reverse() // Reverse so that "command" is last, after all of its parents

            val argsDescription = if (command.argsDescription.isNullOrEmpty()) {
                ""
            } else {
                " &3" + command.argsDescription
            }
            val description = if (command.description.isNullOrEmpty()) {
                ""
            } else {
                " &7- &f" + command.description
            }

            // Join the command stems with spaces (includes current command name)
            val commandStem = commandStems.joinToString(" ")
            return Color.t("&b/$commandStem$argsDescription$description")
        }
    }
}
