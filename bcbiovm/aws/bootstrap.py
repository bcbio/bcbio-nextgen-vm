"""Manage installation and updates of bcbio_vm on AWS systems.
"""
import argparse
import os
import sys

from bcbiovm.aws import icel

def setup_cmd(awsparser):
    parser_sub_b = awsparser.add_parser("bcbio", help="Manage bcbio on AWS systems")
    parser_b = parser_sub_b.add_subparsers(title="[bcbio specific actions]")

    parser = parser_b.add_parser("bootstrap",
                                 help="Update a bcbio AWS system with the latest code and tools",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--econfig", help="Elasticluster bcbio configuration file",
                        default=icel.DEFAULT_EC_CONFIG)
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Emit verbose output when running "
                             "Ansible playbooks")
    parser.set_defaults(func=bootstrap)

def bootstrap(args):
    """Update bcbio_vm on worker nodes.

    Should eventually handle downloading genomes on demand as we support that.
    """
    playbook_path = os.path.join(sys.prefix, "share", "bcbio-vm", "ansible",
                                 "roles", "bcbio_bootstrap", "tasks", "main.yml")
    icel.run_ansible_pb(playbook_path, args)
