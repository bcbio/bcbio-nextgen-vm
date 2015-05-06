"""
Helper class for collecting and processing information regarding
resources usage.
"""
import collections
import os
import re

import pandas
import paramiko
from bcbio.graph import graph

from bcbiovm.aws import icel
from bcbiovm.common import utils

LOG = utils.get_logger(__name__)


class Collector(object):

    """
    Collect from the each instances the files which contains
    information regarding resources consumption.

    ::
        # The instance can be used as a function.
        collector = Collector(config, cluster, rawdir, verbose)
        collector()

        # Or the `meth: run` can be called
        collector.run()
    """

    COLLECTL_PATH = '/var/log/collectl/*.raw.gz'
    NATDevice = 'NATDevice'

    def __init__(self, config, cluster, rawdir, verbose):
        """
        :param config:    elasticluster config file
        :param cluster:   cluster name
        :param rawdir:    directory where to copy raw collectl data files.
        :param verbose:   if is `False` the output will be suppressed
        """
        self._output = rawdir
        self._verbose = verbose
        self._elasticluster = utils.ElastiCluster(config)
        self._cluster = self._elasticluster.get_cluster(cluster)
        self._aws_config = self._elasticluster.get_config(cluster)

        self._private_keys = set()
        self._nodes = []

    def __call__(self):
        """Allows an instance of a class to be called as a function."""
        return self.run()

    @property
    def verbose(self):
        """Whether to increase the verbosity."""
        return self._verbose

    def _get_ssh_client(self, host, user, port=22, bastion_host=None):
        """Setup and return an instance of :class bcbiovm.utils.SSHClient:."""
        policy = (paramiko.client.AutoAddPolicy() if bastion_host
                  else paramiko.client.RejectPolicy())

        ssh_client = utils.SSHClient(host=host, user=user, port=port)
        ssh_client.client.set_missing_host_key_policy(policy)
        ssh_client.client.load_host_keys(self._cluster.known_hosts_file)
        ssh_client.connect(bastion_host=bastion_host)
        return ssh_client

    def _collectl_files(self, ssh_client):
        """Wrapper over `ssh_client.stat`.

        Process information from `stat` output.

        :param ssh_client:  instance of :class bcbiovm.utils.SSHClient:
        :return:            :class collections.namedtuple: with the
                            following fields: atime, mtime, size and path
        """
        stats = collections.namedtuple("FileInfo", ["atime", "mtine", "size",
                                                    "path"])

        for file_info in ssh_client.stat(path=self.COLLECTL_PATH,
                                         format=("%s", "%X", "%Y", "%n")):
            access_time = int(file_info[0])
            modified_time = int(file_info[1])
            size = int(file_info[2])

            yield stats(access_time, modified_time, size, file_info[3])

    def _is_different(self, path, remote):
        """Check if exists differences between the local andthe remote file.

        :path:      the path of the local file
        :remote:    a namedtuple with the information regarding the remote
                    file
        """
        if not os.path.exists(path):
            return True

        if int(os.path.getmtime(path)) != remote.mtine:
            return True

        if os.path.getsize(path) != remote.size:
            return True

        return False

    def _management_target(self):
        """The MGT stores file system configuration information for use by
        the clients and other Lustre components.
        """
        node = self._cluster.get_all_nodes()[0]
        if not node.preferred_ip:
            return None

        ssh_client = self._get_ssh_client(node.preferred_ip, node.image_user)
        disk_info = ssh_client.disk_space("/scratch", ftype="lustre")
        ssh_client.close()

        return None if not disk_info else disk_info[0].split(':')[0]

    def _collect(self, host, user, bastion_host=None):
        """Collect the information from the received host.

        :param host:          the server to connect to
        :param user:          the username to authenticate as (defaults to
                              the current local username)
        :param bastion_host:  the bastion host to connect to
        """
        ssh_client = self._get_ssh_client(host, user, bastion_host)
        for collectl in self._collectl_files(ssh_client):
            destination = os.path.join(self._output,
                                       os.path.basename(collectl.path))
            if not self._is_different(destination, collectl):
                continue
            ssh_client.download_file(collectl.path, destination,
                                     utime=(collectl.atime, collectl.mtime))
        ssh_client.close()

    def _fetch_collectl_lustre(self):
        """Get information from the lustre file system."""
        management_target = self._management_target()
        stack_name = icel.get_stack_name(management_target,
                                         self._aws_config['cloud'])
        if not stack_name:
            # FIXME(alexandrucoman): Raise a custom exception
            return

        icel_hosts = icel.get_instances(stack_name, self._aws_config['cloud'])
        for name, host in icel_hosts.items():
            if name == self.NATDevice:
                continue
            self._collect(host, 'ec2-user',
                          bastion_host=icel_hosts[self.NATDevice])

    def available_nodes(self):
        """The available nodes from the received cluster."""
        if not self._nodes:
            for node in self._cluster.get_all_nodes():
                if node.preferred_ip:
                    self._nodes.append(node)

        return self._nodes

    def private_keys(self):
        """The private keys required to access the nodes from the cluster."""
        if not self._private_keys:
            for cluster_type in self._cluster.nodes:
                for node in self._cluster.nodes[cluster_type]:
                    self._private_keys.add(node.user_key_private)

        return self._private_keys

    def run(self):
        """Collect from the each instances the files which contains
        information regarding resources consumption.
        """
        with utils.SSHAgent(self.private_keys()):
            for node in self.available_nodes():
                self._collect(node.preferred_ip, node.image_user)

            self._fetch_collectl_lustre()


class Parser(object):

    """Parse the files collected by :class Collector:"""

    COLLECTL_SUFFIX = '.raw.gz'

    def __init__(self, bcbio_log, rawdir, verbose):
        """
        :param bcbio_log:   the bcbio log path
        :param rawdir:      directory to put raw data files
        :param verbose:     increase verbosity.
        """
        self._bcbio_log = bcbio_log
        self._rawdir = rawdir
        self._verbose = verbose

    def __call__(self):
        """Allows an instance of a class to be called as a function."""
        return self.run()

    def _time_frame(self):
        """The bcbio running time frame.

        :return:    an instance of :class collections.namedtuple:
                    with the following fields: start and end
        """
        output = collections.namedtuple("Time", ["start", "end"])
        bcbio_timings = graph.get_bcbio_timings(self._bcbio_log)
        steps = bcbio_timings.keys()
        return output(min(steps), max(steps))

    def run(self):
        """Parse the information.

        :return: a tuple with two dictionaries, the first contains
                 an instance of :pandas.DataFrame: for each host and
                 the second one contains information regarding the
                 hardware configuration
        """
        data_frames = {}
        hardware_info = {}
        time_frame = self._time_frame()

        for collectl_file in sorted(os.listdir(self._rawdir)):
            if not collectl_file.endswith(self.COLLECTL_SUFFIX):
                continue

            collectl_path = os.path.join(self._rawdir, collectl_file)
            data, hardware = graph.load_collectl(
                collectl_path, time_frame.start, time_frame.end)

            if len(data) == 0:
                continue

            host = re.sub(r'-\d{8}-\d{6}\.raw\.gz$', '', collectl_file)
            hardware_info[host] = hardware
            if host not in data_frames:
                data_frames[host] = data
            else:
                data_frames[host] = pandas.concat([data_frames[host], data])

        return (data_frames, hardware_info)
