"""Manipulate elasticluster configuration files, providing easy
ways to edit in place.
"""
import abc
import datetime
import shutil

import six

from bcbiovm.client import base
from bcbiovm.common import constant

__all__ = ["EditAWS", "EditAzure"]


@six.add_metaclass(abc.ABCMeta)
class EditConfig(base.BaseCommand):

    """Edit cluster configuration."""

    def __init__(self, parent, parser, name=None):
        super(EditConfig, self).__init__(parent, parser, name)
        self._parser = six.moves.configparser.RawConfigParser()
        self._frontend_section = None
        self._cluster_section = None
        self._frontend = None
        self._cluster = None

    @staticmethod
    def _ask(message, default):
        """Get information from the user."""
        message = "{message} [{default}]".format(message=message,
                                                 default=default)
        value = six.moves.input(message)
        return value or default

    def _parse_section(self, section):
        """Return a directory with all the option available in
        the given section.
        """
        return dict(self._parser.items(section))

    def _backup_config(self):
        """Make a copy of the current config file."""
        now = datetime.datetime.now()
        backup_file = ("%(base)s.bak%(timestamp)s" %
                       {"base": self.args.econfig,
                        "timestamp": now.strftime("%Y-%m-%d-%H-%M-%S")})
        shutil.move(self.args.econfig, backup_file)

    def _save(self):
        """Update the config file."""
        self._backup_config()
        with open(self.args.econfig, "w") as file_handle:
            self._parser.write(file_handle)

    def _instances_types(self):
        """Ask user for information regarding the instances types."""
        compute_nodes = self._ask(
            message=("Number of cluster worker nodes (0 starts a "
                     "single machine instead of a cluster)"),
            default=self._cluster["compute_nodes"])

        if int(compute_nodes) == 0:
            compute_flavor = None
            frontend_flavor = self._ask(
                message="Machine type for single frontend worker node",
                default=self._frontend["flavor"])
        else:
            frontend_flavor = self._ask(
                message="Machine type for frontend worker node",
                default=self._frontend["flavor"])
            compute_flavor = self._ask(
                message="Machine type for compute nodes",
                default=self._cluster["flavor"])

        self._parser.set(self._frontend_section,
                         "flavor", frontend_flavor)
        self._parser.set(self._cluster_section,
                         "compute_nodes", compute_nodes)
        if compute_flavor:
            self._parser.set(self._cluster_section,
                             "flavor", compute_flavor)

    def process(self):
        """Setup parser using the received information."""
        self._parser.read([self.args.econfig])
        self._frontend_section = "cluster/{cluster}/frontend".format(
            cluster=self.args.cluster)
        self._cluster_section = "cluster/{cluster}".format(
            cluster=self.args.cluster)
        self._frontend = self._parse_section(self._frontend_section)
        self._cluster = self._parse_section(self._cluster_section)

        self._instances_types()
        self._process()
        self._save()

    @abc.abstractmethod
    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        pass

    @abc.abstractmethod
    def _process(self):
        """Override this with your desired procedures."""
        pass


class EditAWS(EditConfig):

    """Edit elasticluster setup for AWS provider."""

    def _setup_frontend(self):
        """Change values regarding the frontend node."""
        nfs_size = self._ask(
            message="Size of encrypted NFS mounted filesystem, in Gb",
            default=self._frontend["encrypted_volume_size"])
        # 30 IOPS/Gb, maximum 4000 IOPS http://aws.amazon.com/ebs/details/
        iops = min(int(nfs_size) * 30, 4000)

        self._parser.set(self._frontend_section,
                         "encrypted_volume_size", nfs_size)
        self._parser.set(self._frontend_section,
                         "encrypted_volume_type", "io1")
        self._parser.set(self._frontend_section,
                         "encrypted_volume_iops", iops)

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "edit", help="Edit cluster configuration")
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG.format(
                provider=constant.PROVIDER.AWS))
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.set_defaults(func=self.run)

    def _process(self):
        """Run the command with the received information."""
        self._setup_frontend()
        if self._cluster["compute_nodes"] == 0:
            setup_provider = "ansible"
        else:
            setup_provider = "ansible-slurm"

        self._parser.set(self._cluster_section, "setup_provider",
                         setup_provider)


class EditAzure(EditConfig):

    """Edit elasticluster setup for AWS provider."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "edit", help="Edit cluster configuration")
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG.format(
                provider=constant.PROVIDER.AZURE))
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.set_defaults(func=self.run)

    def _process(self):
        """Run the command with the received information."""
        pass
