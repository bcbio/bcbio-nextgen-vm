from __future__ import print_function

import matplotlib
matplotlib.use('Agg')
import pylab
pylab.rcParams['figure.figsize'] = (35.0, 12.0)

from bcbio import utils
from bcbio.graph import graph as bcbio_graph

from bcbiovm.graph.elasticluster import fetch_collectl


def bootstrap(args):
    if args.cluster and args.cluster.lower() not in ["none", "false"]:
        fetch_collectl(args.econfig, args.cluster,
                       utils.safe_makedir(args.rawdir),
                       args.verbose)


    data, hardware, steps = bcbio_graph.resource_usage(bcbio_log=args.log,
                                                       rawdir=args.rawdir,
                                                       verbose=args.verbose)

    bcbio_graph.generate_graphs(data_frames=data,
                                hardware_info=hardware,
                                steps=steps,
                                outdir=utils.safe_makedir(args.outdir),
                                verbose=args.verbose)
