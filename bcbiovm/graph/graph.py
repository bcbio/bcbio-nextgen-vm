from __future__ import print_function

import matplotlib
matplotlib.use('Agg')
import pylab
pylab.rcParams['figure.figsize'] = (35.0, 12.0)

from bcbio import utils
from bcbio.graph.graph import generate_graphs
from bcbiovm.graph.elasticluster import fetch_collectl


def bootstrap(args):
    if args.cluster and args.cluster.lower() not in ["none", "false"]:
        fetch_collectl(args.econfig, args.cluster,
            utils.safe_makedir(args.rawdir), args.verbose)
    generate_graphs(args.rawdir, args.log, utils.safe_makedir(args.outdir),
        verbose=args.verbose)
