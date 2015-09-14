"""Subcommands available for AWS provider."""
from __future__ import print_function
import argparse
import os
import textwrap

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.docker import install as docker_install
from bcbiovm.provider import factory as cloud_factory
from bcbiovm.provider.aws.clusterk import main as clusterk_main


class ClusterK(base.DockerSubcommand):

    """Run on Amazon web services using Clusterk."""

    def __init__(self, *args, **kwargs):
        super(ClusterK, self).__init__(*args, **kwargs)
        self._need_prologue = True
        self._need_datadir = True

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
        args = docker_install.docker_image_arg(self.args)
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
            "--econfig", help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG.format(provider="aws"))
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
        provider = cloud_factory.get(self.args.provider)()
        provider.bootstrap_iam(config=self.args.econfig,
                               create=not self.args.nocreate,
                               recreate=self.args.recreate)
        if self.args.nocreate:
            print(textwrap.dedent(self.NOIAM_MSG))


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
            default=constant.PATH.EC_CONFIG.format(provider="aws"))
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
        provider = cloud_factory.get(self.args.provider)()
        return provider.bootstrap_vpc(cluster=self.args.cluster,
                                      config=self.args.econfig,
                                      network=self.args.network,
                                      recreate=self.args.recreate)
