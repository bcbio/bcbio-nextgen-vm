"""
Provider base-classes:
    (Beginning of) the contract that cloud providers must follow, and shared
    types that support that contract.
"""
import abc

import six


@six.add_metaclass(abc.ABCMeta)
class BaseCloudProvider(object):

    def __init__(self, name=None):
        self._name = name or self.__class__.__name__
        self._flavor = self._set_flavors()

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
    def information(self, config, cluster, verbose=False):
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
    def colect_data(self, config, cluster, rawdir, verbose):
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
    def bootstrap(self, config, cluster, reboot, verbose):
        """Install or update the the bcbio code and the tools with
        the latest version available.

        :param config:    elasticluster config file
        :param cluster:   cluster name
        :param reboot:    whether to upgrade and restart the host OS
        :param verbose:   increase verbosity
        """
        pass

    def flavors(self, machine=None):
        if not machine:
            return self._flavor.keys()
        else:
            return self._flavor.get(machine)
