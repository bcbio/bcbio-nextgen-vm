"""AWS Cloud Provider for bcbiovm."""

from bcbiovm.common import objects
from bcbiovm.provider import base
from bcbiovm.provider.aws import bootstrap as aws_bootstrap
from bcbiovm.provider.aws import resources as aws_resources


class AWSProvider(base.BaseCloudProvider):

    """AWS Provider for bcbiovm."""

    def __init__(self):
        super(AWSProvider, self).__init__()

    def _set_flavors(self):
        """Returns a dictionary with all the flavors available for the current
        cloud provider.
        """
        return {
            "m3.large": objects.Flavor(cpus=2, memory=3500),
            "m3.xlarge": objects.Flavor(cpus=4, memory=3500),
            "m3.2xlarge": objects.Flavor(cpus=8, memory=3500),
            "c3.large": objects.Flavor(cpus=2, memory=1750),
            "c3.xlarge": objects.Flavor(cpus=4, memory=1750),
            "c3.2xlarge": objects.Flavor(cpus=8, memory=1750),
            "c3.4xlarge": objects.Flavor(cpus=16, memory=1750),
            "c3.8xlarge": objects.Flavor(cpus=32, memory=1750),
            "c4.xlarge": objects.Flavor(cpus=4, memory=1750),
            "c4.2xlarge": objects.Flavor(cpus=8, memory=1750),
            "c4.4xlarge": objects.Flavor(cpus=16, memory=1750),
            "c4.8xlarge": objects.Flavor(cpus=36, memory=1600),
            "r3.large": objects.Flavor(cpus=2, memory=7000),
            "r3.xlarge": objects.Flavor(cpus=4, memory=7000),
            "r3.2xlarge": objects.Flavor(cpus=8, memory=7000),
            "r3.4xlarge": objects.Flavor(cpus=16, memory=7000),
            "r3.8xlarge": objects.Flavor(cpus=32, memory=7000),
        }

    def information(self, config, cluster, verbose=False):
        """
        Get all the information available for this provider.

        The returned information will be used to create a status report
        regarding the bcbio instances.

        :config:          elasticluster config file
        :cluster:         cluster name
        :param verbose:   increase verbosity
        """
        report = aws_resources.Report(config, cluster, verbose)
        report.add_cluster_info()
        report.add_iam_info()
        report.add_security_groups_info()
        report.add_vpc_info()
        report.add_instance_info()
        return report.digest()

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
        collector = aws_resources.Collector(config, cluster, rawdir, verbose)
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
        parser = aws_resources.Parser(bcbio_log, rawdir, verbose)
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
