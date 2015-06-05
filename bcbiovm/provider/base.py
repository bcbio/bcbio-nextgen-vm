"""
Provider base-classes:
    (Beginning of) the contract that cloud providers must follow, and shared
    types that support that contract.
"""
import abc
import os

import paramiko
import six

from bcbiovm.common import cluster as clusterops


@six.add_metaclass(abc.ABCMeta)
class BaseCloudProvider(object):

    _CHMOD = "chmod %(mode)s %(file)s"
    _HOME_DIR = "echo $HOME"
    _SCREEN = "screen -d -m -S %(name)s bash -c '%(script)s &> %(output)s'"

    def __init__(self, name=None):
        self._name = name or self.__class__.__name__
        self._flavor = self._set_flavors()
        self._ecluster = clusterops.ElastiCluster(self._name)

    @abc.abstractmethod
    def _set_flavors(self):
        """Returns a dictionary with all the flavors available for the current
        cloud provider.

        Example:
        ::
            return {
                "m3.large": Flavor(cpus=2, memory=3500),
                "m3.xlarge": Flavor(cpus=4, memory=3500),
                "m3.2xlarge": Flavor(cpus=8, memory=3500),
            }
        """
        pass

    @abc.abstractmethod
    def information(self, cluster, config, verbose=False):
        """
        Get all the information available for this provider.

        The returned information will be used to create a status report
        regarding the bcbio instances.

        :config:    elasticluster config file
        :cluster:   cluster name
        :param verbose:   increase verbosity

        :return:    an instance of :class bcbio.common.objects.Report:
        """
        pass

    @abc.abstractmethod
    def colect_data(self, cluster, config, rawdir, verbose):
        """Collect from the each instances the files which contains
        information regarding resources consumption.

        :param config:    elasticluster config file
        :param cluster:   cluster name
        :param rawdir:    directory where to copy raw collectl data files.
        :param verbose:   if is `False` the output will be suppressed

        Notes:
            The current workflow is the following:
                - establish a SSH connection with the instance
                - get information regarding the `collectl` files
                - copy to local the files which contain new information

            This files will be used in order to generate system statistics
            from bcbio runs. The statistics will contain information regarding
            CPU, memory, network, disk I/O usage.
        """
        pass

    @abc.abstractmethod
    def resource_usage(self, bcbio_log, rawdir, verbose):
        """Generate system statistics from bcbio runs.

        Parse the files obtained by the :meth colect_data: and put the
        information in :class pandas.DataFrame:.

        :param bcbio_log:   local path to bcbio log file written by the run
        :param rawdir:      directory to put raw data files
        :param verbose:     if is `False` the output will be suppressed

        :return: a tuple with two dictionaries, the first contains
                 an instance of :pandas.DataFrame: for each host and
                 the second one contains information regarding the
                 hardware configuration
        :type return: tuple
        """
        pass

    @abc.abstractmethod
    def bootstrap(self, cluster, config, reboot, verbose):
        """Install or update the the bcbio code and the tools with
        the latest version available.

        :param config:    elasticluster config file
        :param cluster:   cluster name
        :param reboot:    whether to upgrade and restart the host OS
        :param verbose:   increase verbosity
        """
        pass

    @staticmethod
    def _execute_remote(connection, command):
        """Execute command on frontend node."""
        try:
            _, stdout, stderr = connection.exec_command(command)
        except paramiko.SSHException as exc:
            return (None, exc)

        return (stdout.read(), stderr.read())

    def flavors(self, machine=None):
        if not machine:
            return self._flavor.keys()
        else:
            return self._flavor.get(machine)

    def start(self, cluster, config=None, no_setup=False, verbose=False):
        """Create a cluster using the supplied configuration.

        :param cluster:   Type of cluster. It refers to a
                          configuration stanza [cluster/<name>].
        :param config:    Elasticluster config file
        :param no_setup:  Only start the cluster, do not configure it.
        :param verbose:   Increase verbosity.
        """
        return self._ecluster.start(cluster, config, no_setup, verbose)

    def stop(self, cluster, config=None, force=False, use_default=False,
             verbose=False):
        """Stop a cluster and all associated VM instances.

        :param cluster:     Type of cluster. It refers to a
                            configuration stanza [cluster/<name>].
        :param config:      Elasticluster config file
        :param force:       Remove the cluster even if not all the nodes
                            have been terminated properly.
        :param use_default: Assume `yes` to all queries and do not prompt.
        :param verbose:     Increase verbosity.
        """
        return self._ecluster.stop(cluster, config, force, use_default,
                                   verbose)

    def setup(self, cluster, config=None, verbose=False):
        """Configure the cluster.

        :param cluster:     Type of cluster. It refers to a
                            configuration stanza [cluster/<name>].
        :param config:      Elasticluster config file
        :param verbose:     Increase verbosity.
        """
        return self._ecluster.setup(cluster, config, verbose)

    def ssh(self, cluster, config=None, ssh_args=None, verbose=False):
        """Connect to the frontend of the cluster using the `ssh` command.

        :param cluster:     Type of cluster. It refers to a
                            configuration stanza [cluster/<name>].
        :param config:      Elasticluster config file
        :ssh_args:          SSH command.
        :param verbose:     Increase verbosity.

        Note:
            If the ssh_args are provided the command will be executed on
            the remote machine instead of opening an interactive shell.
        """
        return self._ecluster.ssh(cluster, config, ssh_args, verbose)

    def run_script(self, cluster, config, script):
        """Run a script on the frontend node inside a screen session.

        :param cluster:     Type of cluster. It refers to a
                            configuration stanza [cluster/<name>].
        :param config:      Elasticluster config file
        :param script:      The path of the script.
        """
        self._ecluster.load_config(config)
        cluster = self._ecluster.get_cluster(cluster)

        frontend = cluster.get_frontend_node()
        client = frontend.connect(known_hosts_file=cluster.known_hosts_file)

        home_dir, _ = self._execute_remote(client, self._HOME_DIR)
        script_name = os.path.basename(script)
        remote_file = os.path.join(home_dir.strip(), script_name)
        ouput_file = ("%(name)s.log" %
                      {"name": os.path.splitext(remote_file)[0]})
        screen_name = os.path.splitext(script_name)[0]

        sftp = client.open_sftp()
        sftp.put(script, remote_file)
        sftp.close()

        self._execute_remote(client, self._CHMOD %
                             {"mode": "a+x", "file": remote_file})
        self._execute_remote(client, self._SCREEN %
                             {"name": screen_name, "script": remote_file,
                              "output": ouput_file})
        client.close()
