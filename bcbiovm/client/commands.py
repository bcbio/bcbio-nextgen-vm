"""The commands used by the command line parser."""
from __future__ import print_function

import argparse

import matplotlib
matplotlib.use('Agg')
import pylab

from bcbio.graph import graph
from bcbio.workflow import template

from bcbiovm import log as logging
from bcbiovm.client import base
from bcbiovm.client.subcommands import aws as aws_subcommand
from bcbiovm.client.subcommands import azure as azure_subcommand
from bcbiovm.client.subcommands import cluster as cluster_subcommand
from bcbiovm.client.subcommands import config as config_subcommand
from bcbiovm.client.subcommands import docker as docker_subcommand
from bcbiovm.client.subcommands import icel as icel_subcommand
from bcbiovm.client.subcommands import tools as tools_subcommand
from bcbiovm.common import constant
from bcbiovm.provider import factory as cloud_factory


LOG = logging.get_logger(__name__)


class Info(base.Command):

    """Information on existing cloud provider."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "info", help="Information on existing cloud provider.")
        parser.add_argument("--econfig", default=None,
                            help="Elasticluster bcbio configuration file")
        parser.add_argument("-c", "--cluster", default="bcbio",
                            help="Elasticluster cluster name")
        parser.add_argument("-v", "--verbose", action="store_true",
                            default=False, help="Emit verbose output")

        parser.set_defaults(work=self.run)

    def work(self):
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


class Graph(base.Command):

    """
    Generate system graphs (CPU/memory/network/disk I/O consumption)
    from bcbio runs.
    """

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
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
        parser.set_defaults(work=self.run)
        return parser

    def work(self):
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
            pylab.rcParams['figure.figsize'] = (35.0, 12.0)
            data, hardware, steps = resource_usage
            graph.generate_graphs(data_frames=data,
                                  hardware_info=hardware,
                                  steps=steps,
                                  outdir=self.args.outdir)


class Template(base.Command):

    """Create a bcbio sample.yaml file from a standard template and inputs."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "template",
            help=("Create a bcbio sample.yaml file from a "
                  "standard template and inputs"))
        parser = template.setup_args(parser)
        parser.add_argument(
            '--relpaths', action='store_true', default=False,
            help="Convert inputs into relative paths to the work directory")

        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        return template.setup


class _Config(base.Container):

    """Manipulate elasticluster configuration files, providing easy
    ways to edit in place.
    """

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "config",
            help="Define configuration details for running a cluster")
        actions = parser.add_subparsers(
            title="[configuration specific actions]")
        self._register_parser("actions", actions)


class ConfigAWS(_Config):

    """Manipulate elasticluster configuration files, providing easy
    ways to edit in place.
    """

    items = [
        (config_subcommand.EditAWS, "actions"),
    ]


class ConfigAzure(_Config):

    """Manipulate elasticluster configuration files, providing easy
    ways to edit in place.
    """

    items = [
        (config_subcommand.EditAzure, "actions"),
    ]


class DockerDevel(base.Container):

    """Utilities to help with develping using bcbion inside of docker."""

    items = [
        (docker_subcommand.Build, "actions"),
        (docker_subcommand.BiodataUpload, "actions"),
        (docker_subcommand.SetupInstall, "actions"),
        (docker_subcommand.SystemUpdate, "actions")
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "devel",
            help=("Utilities to help with develping using bcbion"
                  "inside of docker."))
        actions = parser.add_subparsers(title="[devel commands]")
        self._register_parser("actions", actions)


class ElastiCluster(base.Container):

    """Run and manage a cluster using elasticluster."""

    items = [
        (cluster_subcommand.Bootstrap, "actions"),
        (cluster_subcommand.Start, "actions"),
        (cluster_subcommand.Stop, "actions"),
        (cluster_subcommand.Setup, "actions"),
        (cluster_subcommand.SSHConnection, "actions"),
        (cluster_subcommand.Command, "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "cluster", help="Run and manage AWS clusters")
        actions = parser.add_subparsers(title="[cluster specific actions]")
        self._register_parser("actions", actions)


class ICELCommand(base.Container):

    """Create scratch filesystem using Intel Cloud Edition for Lustre."""

    items = [
        (icel_subcommand.Create, "actions"),
        (icel_subcommand.Specification, "actions"),
        (icel_subcommand.Mount, "actions"),
        (icel_subcommand.Unmount, "actions"),
        (icel_subcommand.Stop, "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "icel",
            help=("Create scratch filesystem using Intel Cloud Edition"
                  "for Lustre"))
        actions = parser.add_subparsers(title="[icel create]")
        self._register_parser("actions", actions)


class AWSProvider(base.Container):

    """Automate resources for running bcbio on AWS."""

    items = [
        (ElastiCluster, "actions"),
        (ConfigAWS, "actions"),
        (Info, "actions"),
        (aws_subcommand.IAMBootstrap, "actions"),
        (aws_subcommand.VPCBoostrap, "actions"),
        (ICELCommand, "actions"),
        (Graph, "actions"),
        (aws_subcommand.ClusterK, "actions")
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "aws",
            help="Automate resources for running bcbio on AWS")
        actions = parser.add_subparsers(title="[aws commands]")
        self._register_parser("actions", actions)


class PrepareEnvironment(base.Container):

    items = [
        (azure_subcommand.ManagementCertificate, "actions"),
        (azure_subcommand.PrivateKey, "actions"),
        (azure_subcommand.ECConfig, "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "prepare",
            help=("Utilities to help with environment configuration."))
        actions = parser.add_subparsers(title="[devel commands]")
        self._register_parser("actions", actions)


class AzureProvider(base.Container):

    """Automate resources for running bcbio on Azure."""
    items = [
        (ElastiCluster, "actions"),
        (ConfigAzure, "actions"),
        (Info, "actions"),
        (Graph, "actions"),
        (PrepareEnvironment, "actions")
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "azure",
            help="Automate resources for running bcbio on Azure")
        actions = parser.add_subparsers(title="[azure commands]")
        self._register_parser("actions", actions)


class Tools(base.Container):

    """Tools and utilities."""

    items = [
        (tools_subcommand.S3Upload, "storage_manager"),
        (tools_subcommand.BlobUpload, "storage_manager"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "tools", help="Tools and utilities.")
        tools = parser.add_subparsers(title="[available tools]")
        upload = tools.add_parser(
            "upload", help="Upload file to a storage manager.")
        storage_manager = upload.add_subparsers(title="[storage manager]")

        self._register_parser("tools", tools)
        self._register_parser("storage_manager", storage_manager)
