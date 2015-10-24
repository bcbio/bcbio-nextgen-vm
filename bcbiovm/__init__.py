"""Run bcbio-nextgen installations inside of virtual machines
and containers.
"""
import os
import sys
import logging

from bcbiovm.common import constant

__all__ = ["config", "log"]


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

    def __init__(self, defaults=None, environment=None):
        if environment is None:
            environment = os.environ.get("BCBIO_ENV", "production")

        self._data = {}
        self._defaults = defaults or {}
        self._environment = constant.ENVIRONMENT.get(environment, {})
        self._namespace = {}

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
        for configurations in (self._defaults, self._environment):
            self._data.update(configurations)
            self._update_namespace(configurations)


class _Logging(object):

    def __init__(self):
        self._loggers = {}

    @classmethod
    def file_handler(cls, handler=None):
        """Setup the file handler."""
        if not config["log.file"]:
            return
        if not handler or handler.baseFilename != config["log.file"]:
            handler = logging.FileHandler(config.log["file"])
            formatter = logging.Formatter(config["log.format"])
            handler.setFormatter(formatter)

        handler.set_name("file_handler")
        handler.setLevel(config["log.file_level"])
        return handler

    @classmethod
    def cli_handler(cls, handler=None):
        """Setup the stream handler."""
        if not handler:
            formatter = logging.Formatter(config["log.format"])
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(formatter)

        handler.set_name("cli_handler")
        handler.setLevel(config["log.cli_level"])
        return handler

    @classmethod
    def _get_handlers(cls, logger):
        handlers = {"cli_handler": None, "file_handler": None}
        for handler in logger.handlers:
            handlers[handler.name] = handler
        return handlers

    def _update_handler(self, name, handler=None):
        getter = getattr(self, name, None)
        return getter(handler) if getter else None

    def _setup_logger(self, logger):
        """Setup the received logger."""
        logger.setLevel(min(config["log.cli_level"],
                            config["log.file_level"]))
        for name, handler in self._get_handlers(logger).items():
            new_handler = self._update_handler(name, handler)
            if new_handler is None and handler:
                handler.flush()
                handler.close()
                logger.removeHandler(handler)
            elif new_handler and handler is None:
                logger.addHandler(new_handler)

    def get_logger(self, name):
        """Obtain a new logger object."""
        if name not in self._loggers:
            logger = logging.getLogger(name)
            logger.propagate = False
            self._setup_logger(logger)
            self._loggers[name] = logger

        return self._loggers[name]

    def update_loggers(self):
        """Update the loggers settings if it is required."""
        for logger in self._loggers.values():
            self._setup_logger(logger)


log = _Logging()
config = _Config(defaults=constant.DEFAULTS)
config.update()
