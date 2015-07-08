"""The commands used by the command line parser."""
from __future__ import print_function
import argparse

import matplotlib
import pylab

from bcbio.graph import graph
from bcbio.workflow import template

from bcbiovm.client import base
from bcbiovm.client.subcommands import factory as command_factory
from bcbiovm.common import constant
from bcbiovm.common import utils
from bcbiovm.provider import factory as cloud_factory


LOG = utils.get_logger(__name__)


class _Config(base.BaseCommand):

    """Manipulate elasticluster configuration files, providing easy
    ways to edit in place.
    """

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "config",
            help="Define configuration details for running a cluster")
        actions = parser.add_subparsers(
            title="[configuration specific actions]")
        self._register_parser("actions", actions)

    def process(self):
        """Run the command with the received information."""
        pass


class Info(base.BaseCommand):

    """Information on existing cloud provider."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "info", help="Information on existing cloud provider.")
        parser.add_argument("--econfig", default=None,
                            help="Elasticluster bcbio configuration file")
        parser.add_argument("-c", "--cluster", default="bcbio",
                            help="Elasticluster cluster name")
        parser.add_argument("-v", "--verbose", action="store_true",
                            default=False, help="Emit verbose output")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider_str = self.args.provider
        provider = cloud_factory.get(provider_str)()
        econf = self.args.econfig or constant.PATH.EC_CONFIG.format(
            provider=provider_str)
        info = provider.information(econf, self.args.cluster,
                                    verbose=self.args.verbose)
        if not info:
            LOG.warning("No info from provider %(provider)s.",
                        {"provider": self.args.provider})
            return
        print(info.text())


class Graph(base.BaseCommand):

    """
    Generate system graphs (CPU/memory/network/disk I/O consumption)
    from bcbio runs.
    """

    groups = None
    sub_commands = None

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "graph",
            help="Generate system graphs (CPU/memory/network/disk I/O "
                 "consumption) from bcbio runs",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "log",
            help="Local path to bcbio log file written by the run.")
        parser.add_argument(
            "-o", "--outdir", default="monitoring/graphs",
            help="Directory to write graphs to.")
        parser.add_argument(
            "-r", "--rawdir", default="monitoring/collectl", required=True,
            help="Directory to put raw collectl data files.")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-e", "--econfig",
            help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG)
        parser.set_defaults(func=self.run)
        return parser

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)()
        if (self.args.cluster and
                self.args.cluster.lower() not in ("none", "false")):
            provider.colect_data(cluster=self.args.cluster,
                                 config=self.args.econfig,
                                 rawdir=self.args.rawdir)

        resource_usage = provider.resource_usage(bcbio_log=self.args.log,
                                                 rawdir=self.args.rawdir)
        if resource_usage:
            matplotlib.use('Agg')
            pylab.rcParams['figure.figsize'] = (35.0, 12.0)
            data, hardware, steps = resource_usage
            graph.generate_graphs(data_frames=data,
                                  hardware_info=hardware,
                                  steps=steps,
                                  outdir=self.args.outdir)


class Template(base.BaseCommand):

    """Create a bcbio sample.yaml file from a standard template and inputs."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "template",
            help=("Create a bcbio sample.yaml file from a "
                  "standard template and inputs"))
        parser = template.setup_args(parser)
        parser.add_argument(
            '--relpaths', action='store_true', default=False,
            help="Convert inputs into relative paths to the work directory")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        return template.setup


class DockerDevel(base.BaseCommand):

    """Utilities to help with develping using bcbion inside of docker."""

    sub_commands = [
        (command_factory.get("docker", "Build"), "actions"),
        (command_factory.get("docker", "BiodataUpload"), "actions"),
        (command_factory.get("docker", "SetupInstall"), "actions"),
        (command_factory.get("docker", "SystemUpdate"), "actions")
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "devel",
            help=("Utilities to help with develping using bcbion"
                  "inside of docker."))
        actions = parser.add_subparsers(title="[devel commands]")
        self._register_parser("actions", actions)

    def process(self):
        """Run the command with the received information."""
        pass


class ElastiCluster(base.BaseCommand):

    """Run and manage a cluster using elasticluster."""

    sub_commands = [
        (command_factory.get("cluster", "Start"), "actions"),
        (command_factory.get("cluster", "Stop"), "actions"),
        (command_factory.get("cluster", "Setup"), "actions"),
        (command_factory.get("cluster", "SSHConnection"), "actions"),
        (command_factory.get("cluster", "Command"), "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "cluster", help="Run and manage AWS clusters")
        actions = parser.add_subparsers(title="[cluster specific actions]")
        self._register_parser("actions", actions)

    def process(self):
        """Run the command with the received information."""
        pass


class ConfigAWS(_Config):

    """Manipulate elasticluster configuration files, providing easy
    ways to edit in place.
    """

    sub_commands = [
        (command_factory.get("config", "EditAWS"), "actions"),
    ]


class ConfigAzure(_Config):

    """Manipulate elasticluster configuration files, providing easy
    ways to edit in place.
    """

    sub_commands = [
        (command_factory.get("config", "EditAzure"), "actions"),
    ]


class ICELCommand(base.BaseCommand):

    """Create scratch filesystem using Intel Cloud Edition for Lustre."""

    sub_commands = [
        (command_factory.get("icel", "Create"), "actions"),
        (command_factory.get("icel", "Specification"), "actions"),
        (command_factory.get("icel", "Mount"), "actions"),
        (command_factory.get("icel", "Unmount"), "actions"),
        (command_factory.get("icel", "Stop"), "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "icel",
            help=("Create scratch filesystem using Intel Cloud Edition"
                  "for Lustre"))
        actions = parser.add_subparsers(title="[icel create]")
        self._register_parser("actions", actions)

    def process(self):
        """Run the command with the received information."""
        pass


class AWSProvider(base.BaseCommand):

    """Automate resources for running bcbio on AWS."""

    sub_commands = [
        (ElastiCluster, "actions"),
        (ConfigAWS, "actions"),
        (Info, "actions"),
        (command_factory.get("aws", "IAMBootstrap"), "actions"),
        (command_factory.get("aws", "VPCBoostrap"), "actions"),
        (ICELCommand, "actions"),
        (Graph, "actions"),
        (command_factory.get("aws", "ClusterK"), "actions")
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "aws",
            help="Automate resources for running bcbio on AWS")
        actions = parser.add_subparsers(title="[aws commands]")
        self._register_parser("actions", actions)

    def process(self):
        """Run the command with the received information."""
        pass


class AzureProvider(base.BaseCommand):

    """Automate resources for running bcbio on Azure."""
    sub_commands = [
        (ElastiCluster, "actions"),
        (ConfigAzure, "actions"),
        (Info, "actions"),
        (Graph, "actions"),
        (command_factory.get("azure", "PrepareEnvironment"), "actions")
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "azure",
            help="Automate resources for running bcbio on Azure")
        actions = parser.add_subparsers(title="[azure commands]")
        self._register_parser("actions", actions)

    def process(self):
        """Run the command with the received information."""
        pass
