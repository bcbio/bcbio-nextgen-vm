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
                "m3.large": Flavor(name="m3.large", cpus=2, memory=3500),
                "m3.xlarge": Flavor(name="m3.xlarge", cpus=4, memory=3500),
                "m3.2xlarge": Flavor(name="m3.2xlarge", cpus=8, memory=3500),
            }
        """
        pass

    @abc.abstractmethod
    def information(self, config, cluster):
        """
        Get all the information available for this provider.

        The returned information will be used to create a status report
        regarding the bcbio instances.

        :config:    elasticluster config file
        :cluster:   cluster name

        The proposed structure for the returned information is the following:
        ::
            {
                # General information regarding the cloud provider
                "meta": {
                    "available_clusters": [],
                    # ...
                }

                # Information regarding the received cluster
                "cluster": {
                    "frontend_node": {
                        "count": 1,
                        "flavor": "m3.2xlarge",
                    },
                    "compute_node": {
                        "count": 1024,
                        "flavor": "m3.2xlarge",
                    },
                    # List of security groups
                    "security_group": []
                    # Private network / Virtual Private Cloud information
                    "private_network": {
                        # ...
                    }
                    # Information regarding instance
                    "instances" : {
                        # instance_name: {
                        #     "instance_state": instance_state
                        #     "ip_address": ip_address,
                        #     [...]
                        # }
                    }
                }

            }
        """
        # NOTE(alexandrucoman): In the current implementation of
        #                       bcbio-nextgen-vm all the information is
        #                       directly printed.

        # TODO(alexandrucoman): Add a custom container for the information.
        # TODO(alexandrucoman): Add a formatter for the container in order
        #                       to display the information
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

    def flavors(self, machine=None):
        if not machine:
            return self._flavor.keys()
        else:
            return self._flavor.get(machine)
