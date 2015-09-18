from __future__ import print_function

import matplotlib
matplotlib.use('Agg')
import os
import pylab
import cPickle as pickle
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

    if args.serialize:
        # Useful to regenerate and slice graphs quickly
        collectl_info = (data, hardware, steps)

        with open(os.path.join(args.outdir, "collectl_info.pickle"), "w") as f:
            pickle.dump(collectl_info, f)
 
    bcbio_graph.generate_graphs(data_frames=data,
                                hardware_info=hardware,
                                steps=steps,
                                outdir=utils.safe_makedir(args.outdir),
                                verbose=args.verbose)
