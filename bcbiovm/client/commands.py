"""The commands used by the command line parser."""
from __future__ import print_function
import argparse
import datetime
import shutil
import textwrap

import getpass
import matplotlib
import pylab
import six

from bcbio.graph import graph

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.provider import factory as cloud_factory


class ConfigEdit(base.BaseCommand):

    """Edit cluster configuration."""

    def __init__(self):
        super(ConfigEdit, self).__init__()
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
        now = datetime.datetime.now()
        backup_file = ("%(base)s.bak%(timestamp)s" %
                       {"base": self.args.econfig,
                        "timestamp": now.strftime("%Y-%m-%d-%H-%M-%S")})
        shutil.move(self.args.econfig, backup_file)
        with open(self.args.econfig, "w") as file_handle:
            self._parser.write(file_handle)

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "edit", help="Edit cluster configuration")
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
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
            frontend_flavor = "c3.large"
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


class Config(base.BaseCommand):

    """Manipulate elasticluster configuration files, providing easy
    ways to edit in place.
    """

    sub_commands = [
        (ConfigEdit, "actions"),
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


class ClusterBootstrap(base.BaseCommand):

    """Update a bcbio AWS system with the latest code and tools."""

    def setup(self):
        parser = self._main_parser.add_parser(
            "bootstrap",
            help="Update a bcbio AWS system with the latest code and tools",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-R", "--no-reboot", default=False, action="store_true",
            help="Don't upgrade the cluster host OS and reboot")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get()
        return provider.bootstrap(cluster=self.args.cluster,
                                  config=self.args.econfig,
                                  reboot=not self.args.no_reboot,
                                  verbose=self.args.verbose)


class ClusterCommand(base.BaseCommand):

    """Run a script on the bcbio frontend node inside a screen session."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "command",
            help="Run a script on the bcbio frontend "
                 "node inside a screen session",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "script", metavar="SCRIPT",
            help="Local path of the script to run. The screen "
                 "session name is the basename of the script.")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get()
        return provider.run_script(cluster=self.args.cluster,
                                   config=self.args.econfig,
                                   script=self.args.script)


class ClusterSetup(base.BaseCommand):

    """Rerun cluster configuration steps."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "setup", help="Rerun cluster configuration steps",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get()
        return provider.setup(cluster=self.args.cluster,
                              config=self.args.econfig,
                              verbose=self.args.verbose)


class ClusterStart(base.BaseCommand):

    """Start a bcbio cluster."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "start", help="Start a bcbio cluster",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-R", "--no-reboot", default=False, action="store_true",
            help="Don't upgrade the cluster host OS and reboot")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get()
        status = provider.start(cluster=self.args.cluster,
                                config=self.args.econfig,
                                no_setup=False,
                                verbose=self.args.verbose)

        if status == 0:
            # Run bootstrap only if the start command successfully runned.
            status = provider.bootstrap(cluster=self.args.cluster,
                                        config=self.args.econfig,
                                        reboot=not self.args.no_reboot,
                                        verbose=self.args.verbose)
        return status


class ClusterStop(base.BaseCommand):

    """Stop a bcbio cluster."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "stop", help="Stop a bcbio cluster",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get()
        return provider.stop(cluster=self.args.cluster,
                             config=self.args.econfig,
                             force=False,
                             use_default=False,
                             verbose=self.args.verbose)


class ClusterSSH(base.BaseCommand):

    """SSH to a bcbio cluster."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "ssh", help="SSH to a bcbio cluster",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.add_argument(
            "args", metavar="ARG", nargs="*",
            help="Execute the following command on the remote "
                 "machine instead of opening an interactive shell.")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get()
        return provider.ssh(cluster=self.args.cluster,
                            config=self.args.econfig,
                            ssh_args=self.args.args,
                            verbose=self.args.verbose)


class ElastiCluster(base.BaseCommand):

    """Run and manage a cluster using elasticluster."""

    sub_commands = [
        (ClusterStart, "actions"),
        (ClusterStop, "actions"),
        (ClusterSetup, "actions"),
        (ClusterSSH, "actions"),
        (ClusterCommand, "actions"),
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


class ICELCreate(base.BaseCommand):

    """Create scratch filesystem using Intel Cloud Edition for Lustre."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "create", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            help=("Create scratch filesystem using "
                  "Intel Cloud Edition for Lustre"))
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
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
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
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
        provider = cloud_factory.get('aws')
        provider.create_icel(
            cluster=self.args.cluster,
            config=self.args.econfig,
            network=self.args.network,
            bucket=self.args.bucket,
            stack_name=self.args.stack_name,
            size=self.args.size,
            oss_count=self.args.oss_count,
            lun_count=self.args.lun_count,
            setup=self.args.setup,
            verbose=self.args.verbose
        )


class ICELMount(base.BaseCommand):

    """Mount Lustre filesystem on all cluster nodes"""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "mount",
            help="Mount Lustre filesystem on all cluster nodes",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.add_argument(
            metavar="STACK_NAME", dest="stack_name", nargs="?",
            default="bcbiolustre",
            help="CloudFormation name for the new stack")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get('aws')
        provider.mount_lustre(cluster=self.args.cluster,
                              config=self.args.econfig,
                              stack_name=self.args.stack_name,
                              verbose=self.args.verbose)


class ICELUnmount(base.BaseCommand):

    """Unmount Lustre filesystem on all cluster nodes."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "unmount",
            help="Unmount Lustre filesystem on all cluster nodes",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.add_argument(
            metavar="STACK_NAME", dest="stack_name", nargs="?",
            default="bcbiolustre",
            help="CloudFormation name for the new stack")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get('aws')
        provider.unmount_lustre(cluster=self.args.cluster,
                                config=self.args.econfig,
                                stack_name=self.args.stack_name,
                                verbose=self.args.verbose)


class ICELStop(base.BaseCommand):

    """Stop the running Lustre filesystem and clean up resources."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "stop",
            help="Stop the running Lustre filesystem and clean up resources",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
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
        provider = cloud_factory.get('aws')
        provider.stop_lustre(cluster=self.args.cluster,
                             config=self.args.econfig,
                             stack_name=self.args.stack_name)


class ICELSpec(base.BaseCommand):

    """Get the filesystem spec for a running filesystem."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "fs_spec",
            help="Get the filesystem spec for a running filesystem",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
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
        provider = cloud_factory.get('aws')
        print(provider.lustre_spec(cluster=self.args.cluster,
                                   config=self.args.econfig,
                                   stack_name=self.args.stack_name))


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


class BoostrapVPC(base.BaseCommand):

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
