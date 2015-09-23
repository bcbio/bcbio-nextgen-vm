"""Tools and utilities used across the client module."""
import os
import sys
import types

import yaml
import six

from bcbiovm.common import exception
from bcbiovm.common import utils
from bcbiovm.common import objectstore

LOG = utils.get_logger(__name__)


def expose(function=None, alias=None):
    """Expose the function, optionally providing an alias or set of aliases.

    This decorator will be use to expose specific methods from a tool.

    Examples:
    ::
        @expose
        def compute_something(self):
            pass

        @expose(alias="something")
        def compute_something(self):
            pass

        @expose(alias=["something", "do_something"])
        def compute_something(self):
            pass

        @expose(["something", "do_something"]):
        def compute_something(self):
            pass
    """

    def get_alias():
        """Return a tuple with the asigned aliases."""
        if alias is None:
            return ()
        elif isinstance(alias, six.string_types):
            return (alias, )
        else:
            return tuple(alias)

    def wrapper(func):
        """Inject information in the wrapped function."""
        func.exposed = True
        func.alias = get_alias()
        return func

    if isinstance(function, (types.FunctionType, types.MethodType)):
        function.exposed = True
        function.alias = get_alias()
        return function

    elif function is None:
        return wrapper

    else:
        alias = function
        return wrapper


class _Node(object):

    """Node from the Tool Tree."""

    def __init__(self, name):
        self._name = name
        self._fields = set()

    def __str__(self):
        """Text representation of the current node."""
        return "<Tool Node {name}>".format(name=self.name)

    @property
    def name(self):
        """Return the name of the current node."""
        return self._name

    @property
    def fields(self):
        """Return all the fields available."""
        return self._fields

    def add_fields(self, field, value):
        """Bind another field to the current node."""
        if field in self.fields:
            LOG.debug("Update the field %(field)r value with: %(value)s",
                      {"field": field, "value": value})
        else:
            LOG.debug("Add field %(field)r with the value %(value)r",
                      {"field": field, "value": value})
            self.fields.add(field)

        setattr(self, field, value)


class Tool(object):

    """Contract class for all tools."""

    def __init__(self):
        self._args = None
        self._name = self.__class__.__name__
        self._namespace = {}
        self._parent = None
        self._alias_tree()

    @property
    def name(self):
        """The name of the current tool."""
        return self._name

    @property
    def namespace(self):
        """All the information related to the namespaces."""
        return self._namespace

    @property
    def args(self):
        """The arguments after the command line was parsed."""
        if self._args is None and self.parent is not None:
            self._args = self.parent.args
        return self._args

    @property
    def parent(self):
        """The object that contains the current tool."""
        return self._parent

    @parent.setter
    def parent(self, value):
        """Update the parent of the current object."""
        self._parent = value

    def _alias_tree(self):
        """Create an alias tree with the exposed methods."""
        for method in dir(self):
            if not getattr("expose", method, False):
                continue

            for alias in getattr("alias", method, (method, )):
                self._insert_alias(method, alias)

    def _insert_alias(self, method, alias):
        """Insert the current method to alias tree."""
        container = self._namespace

        alias_components = alias.split(".")
        root = alias_components.pop()

        for component in alias_components:
            container = container.setdefault(component, {})
        container[root] = method


class Injector(Tool):

    def __init__(self):
        super(Injector, self).__init__()
        self._blacklist = set()

    @property
    def blacklist(self):
        """A collection of invalid alias names."""
        return self._blacklist

    @blacklist.setter
    def blacklist(self, value):
        """Update the values from the blacklist."""
        self._blacklist.update(value)

    def _inject_item(self, parent, namespace, item):
        """Inject properly the received object in the namespace."""
        if isinstance(parent, _Node):
            parent.add_fields(namespace, item)
        else:
            if namespace in self.blacklist:
                LOG.warning("Failed to inject %(item)s in %(namespace)s.",
                            {"item": item, "namespace": namespace})
                return

            setattr(parent, namespace, item)

    def inject(self, tool):
        """Add the current tool in the parent namespace."""
        # Get the first layer
        items = [(namespace, item, self._parent)
                 for namespace, item in tool.namespace]

        while items:
            parent, namespace, item = items.pop()
            if isinstance(item, dict):
                node = getattr(parent, namespace, _Node(namespace))
                self._inject_item(parent, namespace, node)
                for key, value in item.items():
                    items.append(key, value, node)
            else:
                self._inject_item(parent, namespace, item)


class DockerDefaults(Tool):

    """Save and retrieve default locations associated with a
    bcbio-nextgen installation.
    """

    _CONFIG_FILE = "bcbio-docker-config.yaml"
    _CONFIG_DIR = os.getenv('XDG_CONFIG_HOME', os.path.expanduser("~/.config"))
    _DATADIR = os.path.realpath(os.path.normpath(
        os.path.join(os.path.dirname(sys.executable), os.pardir, os.pardir,
                     "data")))
    _BCBIO_SYSTEM = os.path.join(_DATADIR, "galaxy", "bcbio_system.yaml")
    _INSTALL_PARAMS = os.path.join(_DATADIR, "config", "install-params.yaml")

    def __init__(self):
        super(DockerDefaults, self).__init__()
        self._defaults = {"datadir": None}

    @expose(alias=["defaults.config_file"])
    def get_config_file(self, just_filename=False):
        """Retrieve standard user configuration file.

        Uses location from appdirs (https://github.com/ActiveState/appdirs).
        Could pull this in as dependency for more broad platform support.
        """
        config_dir = os.path.join(self._CONFIG_DIR, "bcbio-nextgen")
        config_file = os.path.join(config_dir, self._CONFIG_FILE)

        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

        if just_filename or os.path.exists(config_file):
            return config_file

    @expose(alias=["defaults.get"])
    def get_defaults(self):
        """Retrieve saved default configurations."""
        defaults = {}
        config_file = self.get_config_file()

        if config_file:
            with open(config_file) as in_handle:
                try:
                    defaults = yaml.load(in_handle)
                except ValueError as exc:
                    LOG.exception("Failed to load user configuration "
                                  "file: %(reason)s", {"reason": exc})

        return defaults

    @expose(alias=["defaults.datadir"])
    def get_datadir(self, should_exist=False):
        """Check if the default data directory/standard setup is present."""
        if not should_exist:
            return self._DATADIR

        for path in (self._INSTALL_PARAMS, self._BCBIO_SYSTEM):
            if not os.path.exists(path):
                return None

        return self._DATADIR

    @expose(alias=["defaults.retrieve"])
    def handle_remotes(self):
        """Retrieve supported remote inputs specified on the command line."""
        sample_config = getattr(self.args, "sample_config", None)
        if not sample_config:
            LOG.debug("The sample_config was not provided.")
            return

        if os.path.isfile(sample_config):
            LOG.debug("The sample config file is on the local storage.")
            return

        for manager in (objectstore.AmazonS3, objectstore.AzureBlob):
            if not manager.check_resource(sample_config):
                continue

            LOG.debug("The sample config is remote, using: %(name)s",
                      {"name": manager.__name__})
            self.args.sample_config = manager.load_config(sample_config)
            return

        raise NotImplementedError("Do not recognize remote input %(sample)s" %
                                  {"sample": self.args.sample_config})

    @expose(alias=["defaults.add"])
    def add_defaults(self):
        """Add user configured defaults to supplied command line arguments."""
        config_defaults = self.get_defaults()
        for config, value in self._defaults.items():
            args_value = getattr(self.args, config, None)
            if not args_value or args_value == value:
                if config in config_defaults:
                    setattr(self.args, config, config_defaults[config])

    @expose(alias=["defaults.save"])
    def save_defaults(self):
        """Save user specific defaults to a yaml configuration file."""
        new_config = self.get_defaults()
        for config, value in self._defaults:
            args_value = getattr(self.args, config, None)
            if args_value and args_value != value:
                new_config[config] = args_value

        if new_config:
            config_file = self.get_config_file(just_filename=True)
            with open(config_file, "w") as config_handle:
                yaml.dump(new_config, config_handle, default_flow_style=False,
                          allow_unicode=False)

    @expose(alias=["defaults.datadir"])
    def check_datadir(self, reason=None):
        """Check if the datadir exists if it is required."""
        if self.args.datadir:
            return

        default_datadir = self.get_datadir()
        if default_datadir:
            self.args.datadir = default_datadir
        else:
            raise exception.BCBioException(
                "Must specify a `--datadir` or save the default location "
                "with `saveconfig`. %(reason)s", reason=reason)
