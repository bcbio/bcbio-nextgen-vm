"""Tools and utilities used across the client module."""
import os
import sys

import yaml

from bcbiovm import config as bconfig
from bcbiovm import log as logging
from bcbiovm.common import exception
from bcbiovm.provider.aws import storage as aws_storage
from bcbiovm.provider.azure import storage as azure_storage

LOG = logging.get_logger(__name__)


class Tool(object):

    """Contract class for all tools."""

    def __init__(self, parent=None):
        self._args = None
        self._name = self.__class__.__name__
        self._parent = parent

    @property
    def name(self):
        """The name of the current tool."""
        return self._name

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


class Defaults(Tool):

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

    def __init__(self, parent=None):
        super(Defaults, self).__init__(parent)
        self._defaults = {"datadir": None}

    def _get_config_file(self, just_filename=False):
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

    def _get_defaults(self):
        """Retrieve saved default configurations."""
        defaults = {}
        config_file = self._get_config_file()

        if config_file:
            with open(config_file) as in_handle:
                try:
                    defaults = yaml.load(in_handle)
                except ValueError as exc:
                    LOG.exception("Failed to load user configuration "
                                  "file: %(reason)s", {"reason": exc})

        return defaults

    def _get_datadir(self, should_exist=False):
        """Check if the default data directory/standard setup is present."""
        if not should_exist:
            return self._DATADIR

        for path in (self._INSTALL_PARAMS, self._BCBIO_SYSTEM):
            if not os.path.exists(path):
                return None

        return self._DATADIR

    def retrieve(self):
        """Retrieve supported remote inputs specified on the command line."""
        sample_config = getattr(self.args, "sample_config", None)
        if not sample_config:
            LOG.debug("The sample_config was not provided.")
            return

        if os.path.isfile(sample_config):
            LOG.debug("The sample config file is on the local storage.")
            return

        for manager in (aws_storage.AmazonS3, azure_storage.AzureBlob):
            if not manager.check_resource(sample_config):
                continue

            LOG.debug("The sample config is remote, using: %(name)s",
                      {"name": manager.__name__})
            self.args.sample_config = manager.load_config(sample_config)
            return

        raise NotImplementedError("Do not recognize remote input %(sample)s" %
                                  {"sample": self.args.sample_config})

    def add_defaults(self):
        """Add user configured defaults to supplied command line arguments."""
        config_defaults = self._get_defaults()
        for config, value in self._defaults.items():
            args_value = getattr(self.args, config, None)
            if not args_value or args_value == value:
                if config in config_defaults:
                    setattr(self.args, config, config_defaults[config])

    def save_defaults(self):
        """Save user specific defaults to a yaml configuration file."""
        new_config = self._get_defaults()
        for config, value in self._defaults:
            args_value = getattr(self.args, config, None)
            if args_value and args_value != value:
                new_config[config] = args_value

        if new_config:
            config_file = self._get_config_file(just_filename=True)
            with open(config_file, "w") as config_handle:
                yaml.dump(new_config, config_handle, default_flow_style=False,
                          allow_unicode=False)

    def check_datadir(self, reason=None):
        """Check if the datadir exists if it is required."""
        if self.args.datadir:
            return

        default_datadir = self._get_datadir()
        if default_datadir:
            self.args.datadir = default_datadir
        else:
            raise exception.BCBioException(
                "Must specify a `--datadir` or save the default location "
                "with `saveconfig`. %(reason)s", reason=reason)


class Install(Tool):

    """Retrieve default information required for interacting
    with container images.
    """

    def _get_config_file(self):
        """Retrieve docker image configuration file."""
        config_dir = os.path.join(self.args.datadir, "config")
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

        return os.path.join(config_dir, "install-params.yaml")

    def _get_install_defaults(self):
        """Retrieve saved default configurations."""
        defaults = None
        install_config = self._get_config_file()

        if install_config and os.path.exists(install_config):
            with open(install_config) as in_handle:
                defaults = yaml.load(in_handle)
        return defaults if defaults else {}

    def _add_docker_defaults(self, defaults):
        """Add user configured defaults to supplied command line arguments."""
        if hasattr(self.args, "image") and self.args.image:
            return

        if defaults["image"] and not defaults.get("images") == "None":
            self.args.image = defaults["image"]
        else:
            self.args.image = bconfig.docker["image"]

    def add_install_defaults(self):
        """Add previously saved installation defaults to command line
        arguments.
        """
        default_args = self._get_install_defaults()
        for attribute in ("genomes", "aligners"):
            for default_value in default_args.get(attribute, []):
                current_value = getattr(self.args, attribute)
                if default_value not in getattr(self.args, attribute):
                    current_value.append(default_value)
                setattr(self.args, attribute, current_value)

        self._add_docker_defaults(default_args)

    def image_defaults(self):
        """Add all the missing arguments related to docker image."""
        if hasattr(self.args, "image") and self.args.image:
            return

        default_args = self._get_install_defaults()
        self.args = self._add_docker_defaults(default_args)

    def save_install_defaults(self):
        """Save arguments passed to installation to be used on subsequent upgrades.

        Avoids needing to re-include genomes and aligners on command line.
        """
        current_config = {}
        install_config = self._get_config_file()

        if os.path.exists(install_config):
            with open(install_config) as in_handle:
                current_config = yaml.load(in_handle)

        for attribute in ("genomes", "aligners"):
            if not current_config.get(attribute):
                current_config[attribute] = []

            for value in getattr(self.args, attribute):
                if value not in current_config[attribute]:
                    current_config[attribute].append(str(value))

        if self.args.image and self.args.image != bconfig.docker["image"]:
            current_config["image"] = self.args.image

        with open(install_config, "w") as out_handle:
            yaml.dump(current_config, out_handle, default_flow_style=False,
                      allow_unicode=False)
