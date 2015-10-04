"""
Client base-classes:
    (Beginning of) the contract that commands and parsers must follow.
"""
# pylint: disable=no-self-use

import abc

import six

from bcbiovm.client import tools as client_tools
from bcbiovm.common import exception
from bcbiovm.common import utils

LOG = utils.get_logger(__name__)


@six.add_metaclass(abc.ABCMeta)
class BaseContainer(object):

    """
    Contract class for all the commands or containers.

    :ivar: items: A list which contains (container, metadata) tuples

    Example:
    ::
        class Example(BaseContainer):

            items = [(ExampleOne, metadata), (ExampleTwo, metadata),
                     (ExampleThree, metadata)]
            # ...
    """

    items = None

    def __init__(self):
        self._name = self.__class__.__name__
        self._parsers = {}
        self._containers = []

        # Setup the current job
        self.setup()

        # Bind all the received jobs to the current job
        for container, metadata in self.items or ():
            if not self.check_container(container):
                LOG.error("The container %(container)r is not recognized.",
                          {"container": container})
                continue
            self.register_container(container, metadata)

    @property
    def name(self):
        """Command name."""
        return self._name

    def _register_parser(self, name, item):
        """Register a new item in this container."""
        self._parsers[name] = item

    def _get_parser(self, name):
        """Get an item from the container."""
        try:
            return self._parsers[name]
        except KeyError:
            raise ValueError("Invalid item name %(name)s",
                             {"name": name})

    def task_done(self, result):
        """What to execute after successfully finished processing a task."""
        LOG.info("Execution successful with: %(result)s", result)

    def task_fail(self, exc):
        """What to do when the program fails processing a task."""
        # This should be the default behavior. If the error should
        # be silenced, then it must be done from the derrived class.
        LOG.exception("Failed to run %(name)r: %(reason)s",
                      {"name": self.name, "reason": exc})
        raise exc

    def interrupted(self):
        """What to execute when keyboard interrupts arrive."""
        LOG.warning("Interrupted by the user.")

    def prologue(self):
        """Executed once before the arguments parsing."""
        pass

    def epilogue(self):
        """Executed once after the command running."""
        pass

    @abc.abstractmethod
    def register_container(self, container, metadata):
        """Bind the received container to the current one."""
        pass

    @abc.abstractmethod
    def check_container(self, container):
        """Check if the received container is valid and can be used property.

        Exemple:
        ::
            # ...
            if not isintance(job, Job):
                return False

            return True
        """
        pass

    @abc.abstractmethod
    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        pass

    @abc.abstractmethod
    def work(self):
        """Override this with your desired procedures."""
        pass

    def run(self):
        """Run the command."""
        result = None
        self.prologue()

        try:
            result = self.work()
        except KeyboardInterrupt:
            self.interrupted()
        except exception.BCBioException as exc:
            self.task_fail(exc)
        else:
            self.task_done(result)

        self.epilogue()
        return result


class Command(BaseContainer):

    def __init__(self, parent, parser):
        super(Command, self).__init__()
        self._args = None
        self._command_line = None
        self._parent = parent
        self._parser = parser

        # Add the injector tool to this command
        self._injector = client_tools.Injector()
        # Set the parent value in injector
        self._injector.parent = self
        # Update the values from the injector blacklist
        self._injector.blacklist = dir(self)

    @property
    def parent(self):
        """Return the object that contains the current container."""
        return self._parent

    @property
    def args(self):
        """The arguments after the command line was parsed."""
        if self._args is None:
            self._args = self._discover_attribute("args")
        return self._args

    @property
    def command_line(self):
        """Command line provided to parser."""
        if self._command_line is None:
            self._command_line = self._discover_attribute("command_line")

        return self._command_line

    def _discover_attribute(self, attribute):
        """Search for the received attribute in the command tree."""
        command_tree = [self.parent]
        while command_tree:
            parent = command_tree.pop()
            if hasattr(parent, attribute):
                return getattr(parent, attribute)
            elif parent.parent is not None:
                command_tree.append(parent.parent)

        raise ValueError("The %(attribute)s attribute is missing from the "
                         "client tree." % {"attribute": attribute})

    def _discover_tools(self):
        """Search for all the available tools."""
        for tool_cls in dir(client_tools):
            if not issubclass(tool_cls, client_tools.Tool):
                continue

            # Create a new instance of the received tool
            tool = tool_cls()
            if not tool.namespace:
                LOG.debug("The tool %(tool)r do not expose anything.",
                          {"tool": tool.name})
            self._injector.inject(tool)

    def check_container(self, container):
        """Check if the received container is valid and can be
        used property.
        """
        return True

    def register_container(self, container, metadata):
        """Bind the received container to the current one."""
        pass

    @abc.abstractmethod
    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        pass

    @abc.abstractmethod
    def work(self):
        """Override this with your desired procedures."""
        pass


class Container(BaseContainer):

    """Contract class for all the commands contains.

    :ivar: items: A list which contains (command, parser_name) tuples

    ::
    Example:
    ::
        class Example(Container):

            items = [
                (ExampleOne, "main_parser"),
                (ExampleTwo, "main_parser),
                (ExampleThree, "second_parser")
            ]

            # ...
    """

    def __init__(self, parent, parser):
        super(Container, self).__init__()
        self._parent = parent
        self._parser = parser

    @property
    def parent(self):
        """Return the object that contains the current container."""
        return self._parent

    def check_container(self, container):
        """Check if the received container is valid and can be
        used property.
        """
        if not isinstance(container, Container):
            return False

        return True

    def register_container(self, container, metadata):
        """Bind the received container to the current one."""
        parser = self._get_parser(metadata)
        self._containers.append(container(self, parser))

    def work(self):
        """Override this with your desired procedures."""
        pass

    @abc.abstractmethod
    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        pass


class Client(Container):

    def __init__(self, command_line):
        super(Client, self).__init__(parent=None, parser=None)
        self._command_line = command_line
        self._args = None

    @property
    def args(self):
        """The arguments after the command line was parsed."""
        return self._args

    @property
    def command_line(self):
        """Command line provided to parser."""
        return self._command_line

    @abc.abstractmethod
    def setup(self):
        """Extend the parser configuration in order to expose all
        the received commands.

        Exemple:
        ::
            # ...
            self._parser = argparse.ArgumentParser(
                description=description)
            self._main_parser.add_argument(
                "--example", help="just an example")
            subcommands = self._parser.add_subparsers(
                title="[sub-commands]")
            self._register_parser("subcommands", subcommands)
            # ...
        """
        pass

    def work(self):
        """Parse the command line."""
        self._args = self._parser.parse_args(self.command_line)
        work_function = getattr(self._args, "work", None)
        if not work_function:
            raise exception.NotFound(object="work", container=self._args)

        return work_function()
