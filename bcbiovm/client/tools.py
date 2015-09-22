"""Tools and utilities used across the client module."""
import types

import six

from bcbiovm.common import utils

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
        self._name = self.__class__.__name__
        self._namespace = {}
        self._alias_tree()

    @property
    def name(self):
        """The name of the current tool."""
        return self._name

    @property
    def namespace(self):
        """All the information related to the namespaces."""
        return self._namespace

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
        self._parent = None
        self._blacklist = set()

    @property
    def parent(self):
        """The object that contains the current tool."""
        return self._parent

    @parent.setter
    def parent(self, value):
        """Update the parent of the current object."""
        self._parent = value

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
