"""Manage installation and updates of bcbio_vm on AWS systems.
"""
import argparse
import os

import toolz as tz

from bcbiovm.aws import common

def setup_cmd(awsparser):
    parser_sub_b = awsparser.add_parser("bcbio", help="Manage bcbio on AWS systems")
    parser_b = parser_sub_b.add_subparsers(title="[bcbio specific actions]")

    parser = parser_b.add_parser("bootstrap",
                                 help="Update a bcbio AWS system with the latest code and tools",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser = _add_default_ec_args(parser)
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Emit verbose output when running "
                             "Ansible playbooks")
    parser.set_defaults(func=bootstrap)

    parser = parser_b.add_parser("run",
                                 help="Run a script on the bcbio frontend node inside a screen session",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser = _add_default_ec_args(parser)
    parser.add_argument("script", help=("Path to a script to run. "
                                        "The screen session name is the basename of the script."))
    parser.set_defaults(func=run_remote)

def _add_default_ec_args(parser):
    parser.add_argument("--econfig", help="Elasticluster bcbio configuration file",
                        default=common.DEFAULT_EC_CONFIG)
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    return parser

# ## Bootstrap a new instance

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
    _bootstrap_baseline(args, common.ANSIBLE_BASE)
    _bootstrap_bcbio(args, common.ANSIBLE_BASE)

def _bootstrap_baseline(args, ansible_base):
    """Install required tools -- docker and gof3r on system.
    """
    cluster = common.ecluster_config(args.econfig).load_cluster(args.cluster)
    inventory_path = os.path.join(
        cluster.repository.storage_path,
        'ansible-inventory.{}'.format(args.cluster))

    docker_pb = os.path.join(
        ansible_base, "roles", "docker", "tasks", "main.yml")
    common.run_ansible_pb(inventory_path, docker_pb, args)

    gof3r_pb = os.path.join(
        ansible_base, "roles", "gof3r", "tasks", "main.yml")
    common.run_ansible_pb(inventory_path, gof3r_pb, args)

def _bootstrap_bcbio(args, ansible_base):
    """Install bcbio_vm and docker container with tools. Set core and memory usage.
    """
    cluster = common.ecluster_config(args.econfig).load_cluster(args.cluster)
    inventory_path = os.path.join(
        cluster.repository.storage_path,
        'ansible-inventory.{}'.format(args.cluster))
    playbook_path = os.path.join(
        ansible_base, "roles", "bcbio_bootstrap", "tasks", "main.yml")

    def _calculate_cores_mem(args, cluster_config):
        compute_nodes = int(
            tz.get_in(["nodes", "frontend", "compute_nodes"], cluster_config, 0))
        if compute_nodes > 0:
            machine = tz.get_in(["nodes", "compute", "flavor"], cluster_config)
        else:
            machine = tz.get_in(["nodes", "frontend", "flavor"], cluster_config)
        cores, mem = AWS_INFO[machine]
        # For small number of compute nodes, leave space for runner and controller
        if compute_nodes < 5 and compute_nodes > 0:
            cores = cores - 2
        return {"target_cores": cores, "target_memory": mem}

    common.run_ansible_pb(
        inventory_path, playbook_path, args, _calculate_cores_mem)

# ## Run a remote command

def run_remote(args):
    """Run a script on the frontend node inside a screen session.
    """
    config = common.ecluster_config(args.econfig)
    cluster = config.load_cluster(args.cluster)

    frontend = cluster.get_frontend_node()
    client = frontend.connect(keyfile=cluster.known_hosts_file)

    cmd = "echo $HOME"
    stdin, stdout, stderr = client.exec_command(cmd)
    remote_home = stdout.read().strip()
    remote_file = os.path.join(remote_home, os.path.basename(args.script))
    log_file = "%s.log" % os.path.splitext(remote_file)[0]
    screen_name = os.path.splitext(os.path.basename(remote_file))[0]

    sftp = client.open_sftp()
    sftp.put(args.script, remote_file)
    sftp.close()

    cmd = "chmod a+x %s" % remote_file
    client.exec_command(cmd)
    cmd = "screen -d -m -S %s bash -c '%s &> %s'" % (screen_name, remote_file, log_file)
    stdin, stdout, stderr = client.exec_command(cmd)
    stdout.read()
    client.close()
    print("Running %s on AWS in screen session %s" % (remote_file, screen_name))
