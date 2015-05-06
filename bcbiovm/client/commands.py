"""The commands used by the command line parser."""
import argparse

from bcbio.graph import graph
import matplotlib
import pylab

from bcbiovm.client import base
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
        parser.add_argument(
            "-v", "--verbose", action="store_true", default=False,
            help="Emit verbose output")
        parser.set_defaults(func=self.run)
        return parser

    def process(self):
        """Override this with your desired procedures."""
        # FIXME(alexandrucoman): Properly obtain the cloud provider
        provider = cloud_factory.get('aws')
        if (self.args.cluster and
                self.args.cluster.lower() not in ("none", "false")):
            provider.colect_data(config=self.args.econfig,
                                 cluster=self.args.cluster,
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
