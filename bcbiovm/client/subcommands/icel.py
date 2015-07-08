"""Create scratch filesystem using Intel Cloud Edition for Lustre."""

import argparse

import getpass

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.provider import factory as cloud_factory


class Create(base.BaseCommand):

    """Create scratch filesystem using Intel Cloud Edition for Lustre."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "create", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            help=("Create scratch filesystem using "
                  "Intel Cloud Edition for Lustre"))
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG.format(
                provider=constant.PROVIDER.AWS))
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "--recreate", action="store_true", default=False,
            help=("Remove and recreate the stack, "
                  "destroying all data stored on it"))
        parser.add_argument(
            "--setup", action="store_true", default=False,
            help="Rerun  configuration steps")
        parser.add_argument(
            "-s", "--size", type=int, default="2048",
            help="Size of the Lustre filesystem, in gigabytes")
        parser.add_argument(
            "-o", "--oss-count", type=int, default="4",
            help="Number of OSS nodes")
        parser.add_argument(
            "-l", "--lun-count", type=int, default="4",
            help="Number of EBS LUNs per OSS")
        parser.add_argument(
            "-n", "--network", metavar="NETWORK", dest="network",
            help=("Network (in CIDR notation, a.b.c.d/e) "
                  "to place Lustre servers in"))
        parser.add_argument(
            "-b", "--bucket", default="bcbio-lustre-%s" % getpass.getuser(),
            help="bucket to store generated ICEL template for CloudFormation")
        parser.add_argument(
            metavar="STACK_NAME", dest="stack_name", nargs="?",
            default="bcbiolustre",
            help="CloudFormation name for the new stack")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        # NOTE(alexandrucoman): Command available only for AWS Provider
        provider = cloud_factory.get(constant.PROVIDER.AWS)()
        provider.create_icel(
            cluster=self.args.cluster,
            config=self.args.econfig,
            network=self.args.network,
            bucket=self.args.bucket,
            stack_name=self.args.stack_name,
            size=self.args.size,
            oss_count=self.args.oss_count,
            lun_count=self.args.lun_count,
            setup=self.args.setup
        )


class Mount(base.BaseCommand):

    """Mount Lustre filesystem on all cluster nodes"""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "mount",
            help="Mount Lustre filesystem on all cluster nodes",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG.format(
                provider=constant.PROVIDER.AWS))
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            metavar="STACK_NAME", dest="stack_name", nargs="?",
            default="bcbiolustre",
            help="CloudFormation name for the new stack")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(constant.PROVIDER.AWS)()
        provider.mount_lustre(cluster=self.args.cluster,
                              config=self.args.econfig,
                              stack_name=self.args.stack_name)


class Unmount(base.BaseCommand):

    """Unmount Lustre filesystem on all cluster nodes."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "unmount",
            help="Unmount Lustre filesystem on all cluster nodes",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG.format(
                provider=constant.PROVIDER.AWS))
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            metavar="STACK_NAME", dest="stack_name", nargs="?",
            default="bcbiolustre",
            help="CloudFormation name for the new stack")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(constant.PROVIDER.AWS)()
        provider.unmount_lustre(cluster=self.args.cluster,
                                config=self.args.econfig,
                                stack_name=self.args.stack_name)


class Stop(base.BaseCommand):

    """Stop the running Lustre filesystem and clean up resources."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "stop",
            help="Stop the running Lustre filesystem and clean up resources",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG.format(
                provider=constant.PROVIDER.AWS))
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            metavar="STACK_NAME", dest="stack_name", nargs="?",
            default="bcbiolustre",
            help="CloudFormation name for the new stack")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(constant.PROVIDER.AWS)()
        provider.stop_lustre(cluster=self.args.cluster,
                             config=self.args.econfig,
                             stack_name=self.args.stack_name)


class Specification(base.BaseCommand):

    """Get the filesystem spec for a running filesystem."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "fs_spec",
            help="Get the filesystem spec for a running filesystem",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG.format(
                provider=constant.PROVIDER.AWS))
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            metavar="STACK_NAME", dest="stack_name", nargs="?",
            default="bcbiolustre",
            help="CloudFormation name for the stack")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(constant.PROVIDER.AWS)()
        print(provider.lustre_spec(cluster=self.args.cluster,
                                   config=self.args.econfig,
                                   stack_name=self.args.stack_name))
