"""
Client base-classes:
    (Beginning of) the contract that commands and parsers must follow.
"""

import abc
import collections

import six

from bcbiovm.common import exception

__all__ = ['BaseCommand', 'BaseCommand']


SubCommand = collections.namedtuple("SubCommand",
                                    ["name", "group", "instance"])


@six.add_metaclass(abc.ABCMeta)
class BaseCommand(object):

    """Abstract base class for command."""

    groups = None
    sub_commands = None

    def __init__(self, parrent, name=None):
        self._name = name or self.__class__.__name__.lower()
        self._parent = parrent
        self._groups = {}
        self._commands = {}

        for group, title in self.groups or ():
            self._add_group(group, title)

        for command, group in self.sub_commands or ():
            self._bind(command, group)

        # TODO(alexandrucoman): Check if the parser was set properly
        self._parser = self._get_parser()

    @property
    def args(self):
        """The arguments after the command line was parsed."""
        return self._parent.args

    @property
    def command_line(self):
        """Command line provided to parser."""
        return self._parent.command_line

    @property
    def name(self):
        """Command name."""
        return self._name

    @property
    def subcommands(self):
        """Subcommands generator.

        Each subcommand is an namedtuple with the following fields:
            :name:      the command name
            :group:     the name of the group to which it belongs
            :instance:  an instance of a subclass of BaseCommand
        """
        for group_name, container in self._commands.items():
            for command_name, command in container.items():
                yield SubCommand(command_name, group_name, command)

    def _add_group(self, name, title):
        """Create a container for another commands."""
        subparser = self._parser.add_subparsers(title=title)
        self._groups[name] = subparser

    def _bind(self, command, group_name):
        """Bind another command to one of the created groups."""
        subcommand = command(self)
        command_group = self._commands.setdefault(group_name, {})
        command_group[subcommand.name] = subcommand

    def _get_parser(self):
        """Get the parser for the current command."""
        if isinstance(self._parent, self.__class__):
            # The current command is bonded to another command
            return self._parent.get_group(self.name)
        else:
            # The current command is directly bonded to the parser
            # FIXME(alexandrucoman): Expose the subparser in BaseParser
            pass

    def get_group(self, command_name):
        """Return the group for the received command name."""
        for command in self.subcommands:
            if command.name == command_name:
                return command.group
        return None

    def command_done(self, result):
        """What to execute after successfully finished processing a command."""
        pass

    def command_fail(self, exc):
        """What to do when the program fails processing a command."""
        pass

    def interrupted(self):
        """What to execute when keyboard interrupts arrive."""
        pass

    @abc.abstractmethod
    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        pass

    @abc.abstractmethod
    def process(self):
        """Override this with your desired procedures."""
        pass

    def run(self):
        """Run the command."""
        try:
            result = self.process()
        except KeyboardInterrupt:
            self.interrupted()
        except exception.BCBioException as exc:
            self.command_fail(exc)
        else:
            self.command_done(result)
        return result


@six.add_metaclass(abc.ABCMeta)
class BaseParser(object):

    """Base class for command line parser."""

    commands = None

    def __init__(self, command_line):
        self._args = []
        self._commands = []
        self._command_line = command_line
        self._parser = None

        for command_cls in self.commands:
            self.register_command(command_cls(self))

        self.setup()

    @property
    def args(self):
        """The arguments after the command line was parsed."""
        return self._args

    @property
    def command_line(self):
        """Command line provided to parser."""
        return self._command_line

    def _discover_commands(self, command):
        """Search for all the subcommands for the received command."""
        container = [command]
        while container:
            command = container.pop()
            for subcommand in command.subcommands:
                container.append(subcommand.instance)
                yield subcommand.instance

    def register_command(self, command):
        """Register a new command.

        If the command have another commands bonded, those will be
        also registered.
        """
        if not self.check_command(command):
            return

        self._commands.append(command)
        for subcommand in self._discover_commands(command):
            self._commands.append(subcommand)

    @abc.abstractmethod
    def check_command(self, command):
        """Check if the received command is valid and can be used property.

        Exemple:
        ::
            # ...
            if not isintance(command, AbstractCommand):
                return False

            return True
        """
        pass

    @abc.abstractmethod
    def setup_parser(self):
        """Setup the argument parser.

        Exemple:
        ::
            # ...
            self._parser = argparse.ArgumentParser(description=description)
            self._parser.add_argument("--example", help="just an example")
            # ...
        """
        pass

    def setup(self):
        """Extend the parser configuration in order to expose all
        the received commands.
        """
        self.setup_parser()
        for command in self._commands:
            command.setup()

    def run(self):
        """Parse the command line."""
        # Call prologue handle for all the registered commands
        for command in self._commands:
            command.prologue()

        # Parse the command line
        self._args = self._parser.parse_args(self.command_line)
        # TODO(alexandrucoman): Execute the command

        # Call epilogue handle for all the registered commands
        for command in self._commands:
            command.epilogue()
