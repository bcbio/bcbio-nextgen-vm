"""Run bcbio-nextgen installations inside of virtual machines
and containers.
"""
from bcbiovm.common import constant


class _NameSpace(object):

    def __init__(self, data, name, fields):
        self._data = data
        self._name = name
        self._fields = fields

    def __str__(self):
        """String representation for current task."""
        return "<Namespace {}>".format(self._name)

    def __setattr__(self, name, value):
        """Hook set attribute method for update the received item
        from config.
        """
        if name.startswith("_"):
            self.__dict__[name] = value
        else:
            key = self._key(name)
            self._data[key] = value

    def __setitem__(self, name, value):
        """Hook set item method for update the received item
        from config.
        """
        key = self._key(name)
        self._data[key] = value

    def __getattr__(self, name):
        """Hook for getting attributes from local storage"""
        fields = self.__dict__.get("_fields", ())
        if name in fields:
            key = self._key(name)
            return self._data[key]

        raise AttributeError("'NameSpace' object has no attribute '{}'"
                             .format(name))

    def __getitem__(self, key):
        """Hook for getting items from local storage"""
        key_name = self._key(key)
        if key_name in self._data:
            return self._data[key_name]

        raise KeyError(key)

    def _key(self, field):
        """Return the key name for the received field."""
        return "{}.{}".format(self._name, field)

    def fields(self):
        """The fields available in the current namespace."""
        return list(self._fields)


class _Config(object):

    """Container for global config values."""

    defaults = {}
    environment = "production"

    def __init__(self):
        self._data = {}
        self._namespace = {}
        self._environment = constant.ENVIRONMENT.get(self.environment, {})

    def __str__(self):
        """String representation for current task."""
        return "<Config: {}>".format(self._data.keys())

    def __setitem__(self, name, value):
        """Hook set attribute method for update the received item
        from config.
        """
        if "." in name:
            self._update_namespace((name, ))
            self._data[name] = value

    def __getitem__(self, key):
        """Hook for getting items from local storage"""
        if key in self._data:
            return self._data[key]
        raise KeyError(key)

    def __getattr__(self, name):
        """Hook for getting attributes from local storage"""
        namespace = self.__dict__.get("_namespace")
        if namespace and name in namespace:
            return _NameSpace(self._data, name, namespace[name])

        raise AttributeError("'Config' object has no attribute '{}'"
                             .format(name))

    def _update_namespace(self, configurations):
        """Create the namespaces required by the current configurations."""
        for item in configurations:
            if "." not in item:
                continue
            key, value = item.split('.', 1)
            namaspace = self._namespace.setdefault(key, set())
            namaspace.add(value)

    def update(self):
        """Update fields from local storage."""
        for configurations in (self.defaults, self._environment):
            self._data.update(configurations)
            self._update_namespace(configurations)

config = _Config()
config.defaults = constant.DEFAULTS
config.update()
