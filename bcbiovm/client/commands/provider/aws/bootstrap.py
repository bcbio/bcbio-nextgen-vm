"""Subcommands available for AWS provider."""

from __future__ import print_function
import argparse
import textwrap

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.provider import aws


class IdentityAccessManagement(base.Command):

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
        parser = self._parser.add_parser(
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
        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        provider = aws.AWSProvider()
        provider.bootstrap_iam(config=self.args.econfig,
                               create=not self.args.nocreate,
                               recreate=self.args.recreate)
        if self.args.nocreate:
            print(textwrap.dedent(self.NOIAM_MSG))


class VirtualPrivateCloud(base.Command):

    """Create VPC and associated resources."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
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
        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        provider = aws.AWSProvider()
        return provider.bootstrap_vpc(cluster=self.args.cluster,
                                      config=self.args.econfig,
                                      network=self.args.network,
                                      recreate=self.args.recreate)
