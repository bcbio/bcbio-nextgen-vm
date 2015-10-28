"""
Helper class for updating or installing the bcbio and its requirements.
"""

import collections
import os

import toolz
from bcbio.distributed import ipython

from bcbiovm.common import cluster as clusterops
from bcbiovm.provider.common import playbook as common_playbook


class Bootstrap(object):

    """Update or install the bcbio and its requirements."""

    _RESPONSE = collections.namedtuple("Response",
                                       ["status", "unreachable", "failures"])
    PLAYBOOK_ORDER = ("docker", "gof3r", "nfs", "bcbio")

    def __init__(self, provider, config, cluster_name, reboot, playbook=None):
        """
        :param provider:       an instance of
                               :class bcbiovm.provider.base.BaseCloudProvider:
        :param config:         elasticluster config file
        :param cluster_name:   cluster name
        :param reboot:         whether to upgrade and restart the host OS
        """
        self._config = config
        self._cluster_name = cluster_name
        self._reboot = reboot
        self._provider = provider
        self._playbook = playbook if playbook else common_playbook.Playbook()

        self._ecluster = clusterops.ElastiCluster(provider=self._provider.name)
        self._ecluster.load_config(config)
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
        playbook = clusterops.AnsiblePlaybook(
            inventory_path=self._inventory_path,
            playbook_path=playbook,
            config=self._config,
            cluster=self._cluster_name,
            extra_vars=extra_vars,
            provider=self._provider.name)
        playbook_response = playbook.run()
        return self._RESPONSE(not any(playbook_response), *playbook_response)

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
            flavor = self._provider.get_flavor(machine=machine)
            cores = ipython.per_machine_target_cores(flavor.cpus,
                                                     compute_nodes)
            return {
                "target_cores": cores,
                "target_memory": flavor.memory,
                "upgrade_host_os_and_reboot": self._reboot}

        return self._run_playbook(self._playbook.bcbio, _extra_vars)

    def docker(self):
        """Install docker."""
        return self._run_playbook(self._playbook.docker)

    def gof3r(self):
        """Install gof3r."""
        return self._run_playbook(self._playbook.gof3r)

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

        return self._run_playbook(self._playbook.nfs, _extra_vars)

    def run(self):
        """Install or update the bcbio-nextgen code and the tools
        with the latest version available."""
        result = {}
        for playbook_name in self.PLAYBOOK_ORDER:
            playbook = getattr(self, playbook_name)
            result[playbook_name] = playbook()
            if not result[playbook_name].status:
                break
        return result
