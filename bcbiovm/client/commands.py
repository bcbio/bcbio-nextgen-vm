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
from bcbiovm.provider import factory as cloud_factory


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
        parser.add_argument(
            "-v", "--verbose", action="store_true", default=False,
            help="Emit verbose output")
        parser.set_defaults(func=self.run)
        return parser

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)
        if (self.args.cluster and
                self.args.cluster.lower() not in ("none", "false")):
            provider.colect_data(cluster=self.args.cluster,
                                 config=self.args.econfig,
                                 rawdir=self.args.rawdir,
                                 verbose=self.args.verbose)

        resource_usage = provider.resource_usage(bcbio_log=self.args.log,
                                                 rawdir=self.args.rawdir,
                                                 verbose=self.args.verbose)
        if resource_usage:
            matplotlib.use('Agg')
            pylab.rcParams['figure.figsize'] = (35.0, 12.0)
            data_frames, hardware_info = resource_usage
            # Note(alexandrucoman): For the moment graph.generate_graphs
            #                       do not recognise this argument
            #                       configuration.
            graph.generate_graphs(data_frames, hardware_info, self.args.outdir)


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
        self._register_parser(actions, "actions")

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
        self._register_parser(actions, "actions")

    def process(self):
        """Run the command with the received information."""
        pass


class Config(base.BaseCommand):

    """Manipulate elasticluster configuration files, providing easy
    ways to edit in place.
    """

    sub_commands = [
        (command_factory.get("config", "Edit"), "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "config",
            help="Define configuration details for running a cluster")
        actions = parser.add_subparsers(
            title="[configuration specific actions]")
        self._register_parser(actions, "actions")

    def process(self):
        """Run the command with the received information."""
        pass


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
        self._register_parser(actions, "actions")

    def process(self):
        """Run the command with the received information."""
        pass


class AWSProvider(base.BaseCommand):

    """Automate resources for running bcbio on AWS."""

    sub_commands = [
        (ElastiCluster, "actions"),
        (Config, "actions"),
        # TODO(alexandrucoman): Add `info` command
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
        self._register_parser(actions, "actions")

    def process(self):
        """Run the command with the received information."""
        pass


class Provider(base.BaseCommand):

    sub_commands = [
        (AWSProvider, "provider")
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        subparser = self._main_parser.add_subparsers(title=["cloud provider"],
                                                     dest="provider")
        self._register_parser(subparser, "provider")

    def process(self):
        """Run the command with the received information."""
        pass
