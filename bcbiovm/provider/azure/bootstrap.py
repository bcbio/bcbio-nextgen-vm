"""
Helper class for updating or installing the bcbio and its requirements.
"""
from bcbiovm.provider import base
from bcbiovm.provider import playbook


class Bootstrap(base.Bootstrap):

    """
    Update or install the bcbio and its requirements.
    """

    def __init__(self, provider, config, cluster_name, reboot):
        """
        :param provider:       an instance of
                               :class bcbiovm.provider.base.BaseCloudProvider:
        :param config:         elasticluster config file
        :param cluster_name:   cluster name
        :param reboot:         whether to upgrade and restart the host OS
        """
        super(Bootstrap, self).__init__(provider, config, cluster_name, reboot)
        self._playbook = playbook.AzurePlaybook()

    def docker(self):
        """Install docker."""
        return self._run_playbook(self._playbook.docker)
