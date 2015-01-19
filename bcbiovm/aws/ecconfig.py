"""Manipulate elasticluster configuration files, providing easy ways to edit in place.
"""
from __future__ import print_function

import ConfigParser
import datetime
import shutil

import toolz as tz

from bcbiovm.aws import common

def setup_cmd(awsparser):
    parser_sub_c = awsparser.add_parser("config", help="Define configuration details for running a cluster")
    parser_c = parser_sub_c.add_subparsers(title="[configuration specific actions]")

    parser = parser_c.add_parser("edit", help="Edit cluster configuration")
    parser.set_defaults(func=run_edit)
    parser = common.add_default_ec_args(parser)

def _ask(vals, helpstr, ks):
    default = tz.get_in(ks, vals)
    return raw_input("%s [%s]: " % (helpstr, default)) or default

def run_edit(args):
    parser = ConfigParser.RawConfigParser()
    parser.read([args.econfig])
    vals = {"frontend": {k: v for k, v in parser.items("cluster/%s/frontend" % (args.cluster))},
            "cluster": {k: v for k, v in parser.items("cluster/%s" % (args.cluster))}}
    print("Changing configuration for cluster %s\n" % args.cluster)
    nfs_size = _ask(vals, "Size of NFS mounted filesystem, in Gb", ["frontend", "root_volume_size"])
    compute_nodes = _ask(vals, "Number of cluster worker nodes (0 starts a single machine instead of a cluster)",
                         ["cluster", "compute_nodes"])
    # single machine
    if int(compute_nodes) == 0:
        setup_provider = "ansible"
        frontend_flavor = _ask(vals, "Machine type for single frontend worker node", ["frontend", "flavor"])
        compute_flavor = None
    # cluster
    else:
        setup_provider = "ansible-slurm"
        frontend_flavor = "c3.large"
        compute_flavor = _ask(vals, "Machine type for compute nodes", ["cluster", "flavor"])
    parser.set("cluster/%s/frontend" % args.cluster, "flavor", frontend_flavor)
    parser.set("cluster/%s/frontend" % args.cluster, "root_volume_size", nfs_size)
    parser.set("cluster/%s" % args.cluster, "setup_provider", setup_provider)
    parser.set("cluster/%s" % args.cluster, "compute_nodes", compute_nodes)
    if compute_flavor:
        parser.set("cluster/%s" % args.cluster, "flavor", compute_flavor)

    bak_file = args.econfig + ".bak%s" % datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    shutil.copyfile(args.econfig, bak_file)
    with open(args.econfig, "w") as out_handle:
        parser.write(out_handle)
    print()
    print("Updated configuration for cluster %s" % args.cluster)
    print ("Run 'bcbio_vm.py aws info' to see full details for the cluster")
