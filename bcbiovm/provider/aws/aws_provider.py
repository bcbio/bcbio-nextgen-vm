"""AWS Cloud Provider for bcbiovm."""

from bcbiovm.provider import base
from bcbiovm.provider.aws import bootstrap as aws_bootstrap
from bcbiovm.provider.aws import resources


class AWSProvider(base.BaseCloudProvider):

    def __init__(self):
        super(AWSProvider, self).__init__()

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
        collector = resources.Collector(config, cluster, rawdir, verbose)
        collector.run()

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
        parser = resources.Parser(bcbio_log, rawdir, verbose)
        return parser.run()

    def bootstrap(self, config, cluster, reboot, verbose):
        """Install or update the bcbio-nextgen code and the tools
        with the latest version available.

        :param config:    elasticluster config file
        :param cluster:   cluster name
        :param reboot:    whether to upgrade and restart the host OS
        :param verbose:   increase verbosity
        """
        install = aws_bootstrap.Bootstrap(provider=self, config=config,
                                          cluster_name=cluster, reboot=reboot,
                                          verbose=verbose)
        for playbook in (install.docker, install.gof3r, install.nfs,
                         install.bcbio):
            # TODO(alexandrucoman): Check the results
            playbook()
