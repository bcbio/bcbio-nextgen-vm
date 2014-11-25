"""Manage installation and updates of bcbio_vm on AWS systems.
"""
import argparse
import os
import sys

import toolz as tz

from bcbiovm.aws import common

def setup_cmd(awsparser):
    parser_sub_b = awsparser.add_parser("bcbio", help="Manage bcbio on AWS systems")
    parser_b = parser_sub_b.add_subparsers(title="[bcbio specific actions]")

    parser = parser_b.add_parser("bootstrap",
                                 help="Update a bcbio AWS system with the latest code and tools",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--econfig", help="Elasticluster bcbio configuration file",
                        default=common.DEFAULT_EC_CONFIG)
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Emit verbose output when running "
                             "Ansible playbooks")
    parser.set_defaults(func=bootstrap)

# Core and memory usage for AWS instances
AWS_INFO = {
    "m3.large": [2, 3500],
    "m3.xlarge": [4, 3500],
    "m3.2xlarge": [8, 3500],
    "c3.large": [2, 1750],
    "c3.xlarge": [4, 1750],
    "c3.2xlarge": [8, 1750],
    "c3.4xlarge": [16, 1750],
    "c3.8xlarge": [32, 1750],
    "r3.large": [2, 7000],
    "r3.xlarge": [4, 7000],
    "r3.2xlarge": [8, 7000],
    "r3.4xlarge": [16, 7000],
    "r3.8xlarge": [32, 7000],
}

def bootstrap(args):
    """Bootstrap base machines to get bcbio-vm ready to run.
    """
    playbook_base = os.path.join(sys.prefix, "share", "bcbio-vm", "ansible", "roles")
    _bootstrap_baseline(args, playbook_base)
    _bootstrap_bcbio(args, playbook_base)

def _bootstrap_baseline(args, playbook_base):
    """Install required tools -- docker and gof3r on system.
    """
    docker_pb = os.path.join(playbook_base, "docker", "tasks", "main.yml")
    common.run_ansible_pb(docker_pb, args)
    gof3r_pb = os.path.join(playbook_base, "gof3r", "tasks", "main.yml")
    common.run_ansible_pb(gof3r_pb, args)

def _bootstrap_bcbio(args, playbook_base):
    """Install bcbio_vm and docker container with tools. Set core and memory usage.
    """
    playbook_path = os.path.join(playbook_base, "bcbio_bootstrap", "tasks", "main.yml")
    def _calculate_cores_mem(args, cluster_config):
        compute_nodes = int(tz.get_in(["nodes", "frontend", "compute_nodes"], cluster_config, 0))
        if compute_nodes > 0:
            machine = tz.get_in(["nodes", "compute", "flavor"], cluster_config)
        else:
            machine = tz.get_in(["nodes", "frontend", "flavor"], cluster_config)
        cores, mem = AWS_INFO[machine]
        # For small number of compute nodes, leave space for runner and controller
        if compute_nodes < 5 and compute_nodes > 0:
            cores = cores - 2
        return {"target_cores": cores, "target_memory": mem}
    common.run_ansible_pb(playbook_path, args, _calculate_cores_mem)
