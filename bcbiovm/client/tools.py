"""Tools and utilities used across the client module."""
import types

import six


def expose(function=None, alias=None):
    """
    Expose the function, optionally providing an alias or set of aliases.

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
            return (alias)
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
