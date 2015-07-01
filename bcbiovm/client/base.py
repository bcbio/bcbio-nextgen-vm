"""
Client base-classes:
    (Beginning of) the contract that commands and parsers must follow.
"""

import abc
import collections

import six

from bcbiovm.common import exception
from bcbiovm.common import utils

__all__ = ['BaseCommand', 'BaseParser']

LOG = utils.get_logger(__name__)
SubCommand = collections.namedtuple("SubCommand", ["name", "instance"])


@six.add_metaclass(abc.ABCMeta)
class BaseCommand(object):

    """Abstract base class for command.

    :ivar: sub_commands: A list which contains (command, parser_name) tuples

    ::
    Example:
    ::
        class Example(BaseCommand):

            sub_command = [
                (ExampleOne, "main_parser"),
                (ExampleTwo, "main_parser),
                (ExampleThree, "second_parser")
            ]

            # ...
    """

    sub_commands = None

    def __init__(self, parent, parser, name=None):
        """
        :param parent:  the parent of the current instance
        :param parser:  the parser asigned to this command
        :param name:    the name of the command
        """
        self._name = name or self.__class__.__name__.lower()
        self._parent = parent
        self._main_parser = parser

        self._parsers = {}
        self._commands = {}

        self.setup()

        for command, parser_name in self.sub_commands or ():
            LOG.debug("Trying to bind %(command)r to %(parser)r",
                      {"command": command.__name__, "parser": parser_name})
            parser = self._get_parser(parser_name)
            self._bind(command, parser)

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
        """Generator for all the commands bonded to the current command.

        Each subcommand is an namedtuple with the following fields:
            :name:      the command name
            :instance:  an instance of a subclass of BaseCommand
        """
        for command_name, command in self._commands.items():
            yield SubCommand(command_name, command)

    def _bind(self, command, parser):
        """Bind another command to the current command and provide
        it a parser.
        """
        subcommand = command(self, parser)
        self._commands[subcommand.name] = subcommand

    def _get_parser(self, name):
        """Get the parser for the current command."""
        try:
            return self._parsers[name]
        except KeyError:
            raise ValueError("Invalid parser name %(name)s",
                             {"name": name})

    def _register_parser(self, name, parser):
        """Register a new parser."""
        self._parsers[name] = parser

    def command_done(self, result):
        """What to execute after successfully finished processing a command."""
        LOG.info("Execution successful with: %(result)s", result)

    def command_fail(self, exc):
        """What to do when the program fails processing a command."""
        # This should be the default behavior. If the error should
        # be silenced, then it must be done from the derrived class.
        raise exc

    def interrupted(self):
        """What to execute when keyboard interrupts arrive."""
        LOG.warning("Interrupted by the user.")

    def prologue(self):
        """Executed once before the arguments parsing."""
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
        self._subparser = None

        self.setup()

        for command_cls in self.commands:
            LOG.debug("Create an instance of %(command)r",
                      {"command": command_cls})
            self.register_command(command_cls(self, self._subparser))

    @property
    def args(self):
        """The arguments after the command line was parsed."""
        return self._args

    @property
    def command_line(self):
        """Command line provided to parser."""
        return self._command_line

    @staticmethod
    def _discover_commands(command):
        """Search for all the subcommands for the received command."""
        container = [command]
        while container:
            command = container.pop()
            LOG.debug("Searching for all the subcommands for %(command)r",
                      {"command": command.name})
            for subcommand in command.subcommands:
                container.append(subcommand.instance)
                LOG.debug("Subcommand %(subcommand)r was found.",
                          {"subcommand": subcommand.name})
                yield subcommand.instance

    def register_command(self, command):
        """Register a new command.

        If the command have another commands bonded, those will be
        also registered.
        """
        LOG.debug("Trying to register %(command)r",
                  {"command": command.name})
        if not self.check_command(command):
            LOG.error("%(command)r is not recognized.",
                      {"command": command})
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
    def setup(self):
        """Extend the parser configuration in order to expose all
        the received commands.

        Exemple:
        ::
            # ...
            self._parser = argparse.ArgumentParser(
                description=description)
            self._parser.add_argument(
                "--example", help="just an example")
            self._subparser = self._parser.add_subparsers(
                title="[sub-commands]")
            # ...
        """

    def run(self):
        """Parse the command line."""
        # Call prologue handle for all the registered commands
        for command in self._commands:
            command.prologue()

        # Parse the command line
        self._args = self._parser.parse_args(self.command_line)
        # pylint: disable=no-member
        return self._args.func()
