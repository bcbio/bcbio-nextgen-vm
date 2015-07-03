"""Run bcbio-nextgen installations inside of virtual machines
and containers.
"""


class Config(object):

    """Container for global config values."""

    def __init__(self, config=None):
        self._data = {}

        if config:
            self.update(config)

    def __str__(self):
        """String representation for current task."""
        return "<Config: {}>".format(self._data.keys())

    def __setattr__(self, name, value):
        """Hook set attribute method for update the received item
        from config.
        """
        if name == "_data":
            self.__dict__[name] = value
            return

        container = getattr(self, "_data", None)
        if container:
            container[name] = value

    def __getattr__(self, name):
        """Hook for getting attribute from local storage"""
        container = self.__dict__.get("_data")
        if container and name in container:
            return container[name]

        raise AttributeError("'Config' object has no attribute '{}'"
                             .format(name))

    def update(self, config):
        """Update fields from local storage."""
        if not isinstance(config, dict):
            raise ValueError("Argument `config` should be dictionary")
        self._data.update(config)

config = Config()
