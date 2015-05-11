"""
Helper class for updating or installing the bcbio and its requirements.
"""
import os

import toolz

from bcbio.distributed import ipython

from bcbiovm.common import cluster
from bcbiovm.common import constant


class Bootstrap(object):

    """
    Update or install the bcbio and its requirements.
    """

    def __init__(self, provider, config, cluster_name, reboot, verbose):
        """
        :param provider:       an instance of
                               :class bcbiovm.provider.base.BaseCloudProvider:
        :param config:         elasticluster config file
        :param cluster_name:   cluster name
        :param reboot:         whether to upgrade and restart the host OS
        :param verbose:        increase verbosity
        """
        self._config = config
        self._cluster_name = cluster_name
        self._reboot = reboot
        self._verbose = verbose
        self._provider = provider

        self._ecluster = cluster.ElastiCluster(config)
        self._cluster = self._ecluster.get_cluster(cluster_name)

        self._inventory_path = os.path.join(
            self._cluster.repository.storage_path,
            "ansible-inventory.%(cluster)s" % {"cluster": cluster_name})

    def _run_playbook(self, playbook, extra_vars=None):
        """Run a playbook and return the result.

        :param playbook_path:   the path to a playbook file
        :param extra_args:      is an option function that should return
                                extra variables to pass to ansible given
                                the arguments and cluster configuration
        """
        playbook = cluster.AnsiblePlaybook(inventory_path=self._inventory_path,
                                           playbook_path=playbook,
                                           config=self._config,
                                           cluster=self._cluster_name,
                                           verbose=self._verbose,
                                           extra_vars=extra_vars)
        return playbook.run()

    def docker(self):
        """Install docker."""
        return self._run_playbook(constant.PLAYBOOK.DOCKER)

    def gof3r(self):
        """Install gof3r."""
        return self._run_playbook(constant.PLAYBOOK.GOF3R)

    def bcbio(self):
        """Install bcbio_vm and docker container with tools.
        Set core and memory usage.
        """
        def _extra_vars(cluster_config):
            """Extra variables to inject into a playbook."""
            # Calculate cores and memory
            compute_nodes = int(
                toolz.get_in(["nodes", "frontend", "compute_nodes"],
                             cluster_config, 0))
            if compute_nodes > 0:
                machine = toolz.get_in(["nodes", "compute", "flavor"],
                                       cluster_config)
            else:
                machine = toolz.get_in(["nodes", "frontend", "flavor"],
                                       cluster_config)
            flavor = self._provider.flavors(machine=machine)
            cores = ipython.per_machine_target_cores(flavor.cpus,
                                                     compute_nodes)
            return {
                "target_cores": cores,
                "target_memory": flavor.memory,
                "upgrade_host_os_and_reboot": self._reboot}

        return self._run_playbook(constant.PLAYBOOK.BCBIO, _extra_vars)

    def nfs(self):
        """Mount encrypted NFS volume on master node and expose
        across worker nodes.
        """
        nfs_server = None
        nfs_clients = []

        with open(self._inventory_path) as file_handle:
            for line in file_handle.readlines():
                if line.startswith("frontend"):
                    nfs_server = line.split()[0]
                elif line.startswith("compute"):
                    nfs_clients.append(line.split()[0])

        def _extra_vars(cluster_config):
            """Extra variables to inject into a playbook."""
            return {
                "encrypted_mount": "/encrypted",
                "nfs_server": nfs_server,
                "nfs_clients": ",".join(nfs_clients),
                "login_user": toolz.get_in(["nodes", "frontend", "login"],
                                           cluster_config),
                "encrypted_device": toolz.get_in(
                    ["nodes", "frontend", "encrypted_volume_device"],
                    cluster_config, "/dev/xvdf")}

        return self._run_playbook(constant.PLAYBOOK.NFS, _extra_vars)
