"""The commands used by the command line parser."""
import argparse

from bcbio.graph import graph
import matplotlib
import pylab

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.provider import factory as cloud_factory


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
