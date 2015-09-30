from __future__ import print_function

import matplotlib
matplotlib.use('Agg')
import os
import pylab
pylab.rcParams['figure.figsize'] = (35.0, 12.0)

from bcbio import utils
from bcbio.graph import graph as bcbio_graph

from bcbiovm.graph.elasticluster import fetch_collectl


def bootstrap(args):
    if args.cluster and args.cluster.lower() not in ["none", "false"]:
        fetch_collectl(args.econfig, args.cluster, args.log,
                       utils.safe_makedir(args.rawdir),
                       args.verbose)

    data, hardware, steps = bcbio_graph.resource_usage(bcbio_log=args.log,
                                                       cluster=args.cluster,
                                                       rawdir=args.rawdir,
                                                       verbose=args.verbose)

    # Collectl_info is cleaned up data ready to be plotted/mangled
    collectl_info = bcbio_graph.generate_graphs(data_frames=data,
                                                hardware_info=hardware,
                                                steps=steps,
                                                outdir=utils.safe_makedir(args.outdir),
                                                verbose=args.verbose)

    if args.serialize:
        pre_graph_info = (data, hardware, steps)
        bcbio_graph.serialize_plot_data(collectl_info, pre_graph_info, args.outdir, "collectl_info.pickle.gz")
