"""Manipulate elasticluster configuration files, providing easy
ways to edit in place.
"""
import abc
import datetime
import shutil

import six

from bcbiovm.client import base
from bcbiovm.common import constant

__all__ = ["EditAWS"]


@six.add_metaclass(abc.ABCMeta)
class EditSetup(base.BaseCommand):

    """Edit cluster configuration."""

    def __init__(self, parent, parser, name=None):
        super(EditSetup, self).__init__(parent, parser, name)
        self._parser = six.moves.configparser.RawConfigParser()

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

    @abc.abstractmethod
    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        pass

    @abc.abstractmethod
    def process(self):
        """Override this with your desired procedures."""
        pass


class EditAWS(EditSetup):

    """Edit elasticluster setup for AWS provider."""

    def _frontend(self, flavor, nfs_size, iops):
        """Change values regarding the frontend node."""
        section = "cluster/{cluster}/frontend".format(
            cluster=self.args.cluster)
        self._parser.set(section, "flavor", flavor)
        self._parser.set(section, "encrypted_volume_size", nfs_size)
        self._parser.set(section, "encrypted_volume_type", "io1")
        self._parser.set(section, "encrypted_volume_iops", iops)

    def _compute_nodes(self, setup_provider, nodes, flavor):
        """Change values regarding the compute nodes."""
        section = "cluster/{cluster}".format(cluster=self.args.cluster)
        self._parser.set(section, "setup_provider", setup_provider)
        self._parser.set(section, "compute_nodes", nodes)
        if flavor:
            self._parser.set(section, "flavor", flavor)

    def _save(self):
        """Update the config file."""
        self._backup_config()
        with open(self.args.econfig, "w") as file_handle:
            self._parser.write(file_handle)

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "edit", help="Edit cluster configuration")
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG.format(provider="aws"))
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        self._parser.read([self.args.econfig])
        frontend = self._parse_section("cluster/%(cluster)s/frontend" %
                                       {"cluster": self.args.cluster})
        cluster = self._parse_section("cluster/%(cluster)s" %
                                      {"cluster": self.args.cluster})
        nfs_size = self._ask(
            message="Size of encrypted NFS mounted filesystem, in Gb",
            default=frontend["encrypted_volume_size"])
        compute_nodes = self._ask(
            message=("Number of cluster worker nodes (0 starts a "
                     "single machine instead of a cluster)"),
            default=cluster["compute_nodes"])

        if int(compute_nodes) == 0:
            compute_flavor = None
            setup_provider = "ansible"
            frontend_flavor = self._ask(
                message="Machine type for single frontend worker node",
                default=frontend["flavor"])
        else:
            setup_provider = "ansible-slurm"
            frontend_flavor = self._ask(
                message="Machine type for frontend worker node",
                default="c3.large")
            compute_flavor = self._ask(
                message="Machine type for compute nodes",
                default=cluster["flavor"])
        self._frontend(
            flavor=frontend_flavor, nfs_size=nfs_size,
            # 30 IOPS/Gb, maximum 4000 IOPS http://aws.amazon.com/ebs/details/
            iops=min(int(nfs_size) * 30, 4000))
        self._compute_nodes(setup_provider=setup_provider, nodes=compute_nodes,
                            flavor=compute_flavor)
        self._save()
