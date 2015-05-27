"""Azure Cloud Provider for bcbiovm."""

from bcbiovm.provider import base

# pylint: disable=no-self-use


class AzureProvider(base.BaseCloudProvider):

    """Azure Provider for bcbiovm."""

    def __init__(self):
        super(AzureProvider, self).__init__()

    def _set_flavors(self):
        """Returns a dictionary with all the flavors available for the current
        cloud provider.
        """
        return {}

    def information(self, config, cluster, verbose=False):
        """
        Get all the information available for this provider.

        The returned information will be used to create a status report
        regarding the bcbio instances.

        :config:          elasticluster config file
        :cluster:         cluster name
        :param verbose:   increase verbosity
        """
        pass

    def colect_data(self, config, cluster, rawdir, verbose):
        """Collect from the each instances the files which contains
        information regarding resources consumption.

        :param config:    elasticluster config file
        :param cluster:   cluster name
        :param rawdir:    directory where to copy raw collectl data files.
        :param verbose:   increase verbosity

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

    def resource_usage(self, bcbio_log, rawdir, verbose):
        """Generate system statistics from bcbio runs.

        Parse the files obtained by the :meth colect_data: and put the
        information in :class pandas.DataFrame:.

        :param bcbio_log:   local path to bcbio log file written by the run
        :param rawdir:      directory to put raw data files
        :param verbose:     increase verbosity

        :return: a tuple with two dictionaries, the first contains
                 an instance of :pandas.DataFrame: for each host and
                 the second one contains information regarding the
                 hardware configuration
        :type return: tuple
        """
        pass

    def bootstrap(self, config, cluster, reboot, verbose):
        """Install or update the bcbio-nextgen code and the tools
        with the latest version available.

        :param config:    elasticluster config file
        :param cluster:   cluster name
        :param reboot:    whether to upgrade and restart the host OS
        :param verbose:   increase verbosity
        """
        pass
