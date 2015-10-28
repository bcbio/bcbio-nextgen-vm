"""Commands without a special group."""
from __future__ import print_function

import matplotlib
matplotlib.use('Agg')
import pylab
from bcbio.graph import graph
from bcbio.workflow import template

from bcbiovm import log as logging
from bcbiovm.client import base
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
                 "consumption) from bcbio runs")
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
