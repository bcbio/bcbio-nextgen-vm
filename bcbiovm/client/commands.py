"""The commands used by the command line parser."""
from __future__ import print_function
import argparse
import os
import textwrap

import matplotlib
import pylab

from bcbio.graph import graph

from bcbiovm.client import base
from bcbiovm.client.subcommands import factory as command_factory
from bcbiovm.common import constant
from bcbiovm.docker import defaults as docker_defaults
from bcbiovm.docker import install as docker_install
from bcbiovm.provider import factory as cloud_factory
from bcbiovm.provider.aws.clusterk import main as clusterk_main


class ClusterK(base.BaseCommand):

    """Run on Amazon web services using Clusterk."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "clusterk",
            help="Run on Amazon web services using Clusterk.")
        parser.add_argument(
            "sample_config",
            help="YAML file with details about samples to process.")
        parser.add_argument(
            "--fcdir",
            help="A directory of Illumina output or fastq files to process",
            type=lambda path: (os.path.abspath(os.path.expanduser(path))))
        parser.add_argument(
            "--systemconfig",
            help=("Global YAML configuration file specifying system details. "
                  "Defaults to installed bcbio_system.yaml."))
        parser.add_argument(
            "-n", "--numcores", type=int, default=1,
            help="Total cores to use for processing")
        parser.add_argument(
            "run_bucket",
            help="Name of the S3 bucket to use for storing run information")
        parser.add_argument(
            "biodata_bucket",
            help=("Name of the S3 bucket to use for "
                  "storing biodata like genomes"))
        parser.add_argument(
            "-q", "--queue", default="default",
            help="Clusterk queue to run jobs on.")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        args = docker_defaults.update_check_args(
            self.args, "Could not run Clusterk parallel analysis.")
        args = docker_install.docker_image_arg(args)
        clusterk_main.run(args, constant.DOCKER)


class IAMBootstrap(base.BaseCommand):

    """Create IAM user and policies."""

    NOIAM_MSG = """
        IAM users and instance profiles not created.
        Manually add the following items to your configuration file:
            ec2_access_key    AWS Access Key ID, ideally generated for an IAM
                              user with full AWS permissions:
                                - http://goo.gl/oe70TE
                                - http://goo.gl/dAJORA
            ec2_secret_key    AWS Secret Key ID matching the ec2_access_key.
            instance_profile  Create an IAM Instance profile allowing access
                              to S3 buckets for pushing/pulling data.
                              Use 'InstanceProfileName' from aws iam
                              list-instance-profiles after setting up:
                                - http://goo.gl/Oa92Y8
                                - http://goo.gl/fhnq5S
                                - http://j.mp/iams3ip

        The IAM user you create will need to have access permissions for:
            - EC2 and VPC
                -- ec2:*
            - IAM instance profiles
                -- iam:PassRole, iam:ListInstanceProfiles
            - CloudFormation for launching a Lutre ICEL instance:
                -- cloudformation:*
        """

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "iam", help="Create IAM user and policies")
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "--recreate", action="store_true", default=False,
            help="Recreate current IAM user access keys")
        parser.add_argument(
            "--nocreate", action="store_true", default=False,
            help=("Do not create a new IAM user, just generate a configuration"
                  " file. Useful for users without full permissions to IAM."))
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        # NOTE(alexandrucoman): Command available only for AWS Provider
        provider = cloud_factory.get('aws')
        provider.bootstrap_iam(config=self.args.econfig,
                               create=not self.args.nocreate,
                               recreate=self.args.recreate)
        if self.args.nocreate:
            print(textwrap.dedent(self.NOIAM_MSG))


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
        provider = cloud_factory.get()
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


class VPCBoostrap(base.BaseCommand):

    """Create VPC and associated resources."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "vpc",
            help="Create VPC and associated resources",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG)
        parser.add_argument(
            "--recreate", action="store_true", default=False,
            help=("Remove and recreate the VPC, destroying all "
                  "AWS resources contained in it."))
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-n", "--network", default="10.0.0.0/16",
            help="network to use for the VPC, in CIDR notation (a.b.c.d/e)")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        # NOTE(alexandrucoman): Command available only for AWS Provider
        provider = cloud_factory.get('aws')
        return provider.bootstrap_vpc(cluster=self.args.cluster,
                                      config=self.args.econfig,
                                      network=self.args.network,
                                      recreate=self.args.recreate)


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


class AWSCommand(base.BaseCommand):

    """Automate resources for running bcbio on AWS."""

    sub_commands = [
        (ElastiCluster, "actions"),
        (Config, "actions"),
        # TODO(alexandrucoman): Add `info` command
        (IAMBootstrap, "actions"),
        (VPCBoostrap, "actions"),
        (ICELCommand, "actions"),
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
