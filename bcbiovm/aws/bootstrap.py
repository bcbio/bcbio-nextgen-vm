"""Manage installation and updates of bcbio_vm on AWS systems.
"""
import os

import toolz as tz

from bcbio.distributed import ipython
from bcbiovm.aws import common

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
    "c4.large": [2, 1750],
    "c4.xlarge": [4, 1750],
    "c4.2xlarge": [8, 1750],
    "c4.4xlarge": [16, 1750],
    "c4.8xlarge": [36, 1600],
    "r3.large": [2, 7000],
    "r3.xlarge": [4, 7000],
    "r3.2xlarge": [8, 7000],
    "r3.4xlarge": [16, 7000],
    "r3.8xlarge": [32, 7000],
    "t2.nano": [1, 500],
    "t2.micro": [1, 1000],
    "t2.small": [1, 2000],
    "t2.medium": [2, 2000],
    "t2.large": [2, 4000],
    "m4.large": [2, 4000],
    "m4.xlarge": [4, 4000],
    "m4.2xlarge": [8, 4000],
    "m4.4xlarge": [16, 4000],
    "m4.10xlarge": [40, 4000],
}

def bootstrap(args):
    """Bootstrap base machines to get bcbio-vm ready to run.
    """
    _bootstrap_baseline(args, common.ANSIBLE_BASE)
    _bootstrap_nfs(args, common.ANSIBLE_BASE)
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

    def _extra_vars(args, cluster_config):
        # Calculate cores and memory
        compute_nodes = int(
            tz.get_in(["nodes", "frontend", "compute_nodes"], cluster_config, 0))
        if compute_nodes > 0:
            machine = tz.get_in(["nodes", "compute", "flavor"], cluster_config)
        else:
            machine = tz.get_in(["nodes", "frontend", "flavor"], cluster_config)
        cores, mem = AWS_INFO[machine]
        cores = ipython.per_machine_target_cores(cores, compute_nodes)
        return {"target_cores": cores, "target_memory": mem,
                "upgrade_host_os_and_reboot": not args.no_reboot}

    common.run_ansible_pb(
        inventory_path, playbook_path, args, _extra_vars)

def _bootstrap_nfs(args, ansible_base):
    """Mount encrypted NFS volume on master node and expose across worker nodes.
    """
    cluster = common.ecluster_config(args.econfig).load_cluster(args.cluster)
    inventory_path = os.path.join(cluster.repository.storage_path,
                                  'ansible-inventory.{}'.format(args.cluster))
    nfs_clients = []
    with open(inventory_path) as in_handle:
        for line in in_handle:
            if line.startswith("frontend"):
                nfs_server = line.split()[0]
            elif line.startswith("compute"):
                nfs_clients.append(line.split()[0])
    playbook_path = os.path.join(ansible_base, "roles", "encrypted_nfs", "tasks", "main.yml")
    def _extra_vars(args, cluster_config):
        return {"encrypted_mount": "/encrypted",
                "nfs_server": nfs_server,
                "nfs_clients": ",".join(nfs_clients),
                "login_user": tz.get_in(["nodes", "frontend", "login"], cluster_config),
                "encrypted_device": tz.get_in(["nodes", "frontend", "encrypted_volume_device"],
                                              cluster_config, "/dev/xvdf")}
    common.run_ansible_pb(inventory_path, playbook_path, args, _extra_vars)
