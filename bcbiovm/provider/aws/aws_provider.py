"""AWS Cloud Provider for bcbiovm."""
# pylint: disable=no-self-use

import os

from bcbio.distributed import objectstore

from bcbiovm import log as logging
from bcbiovm.common import objects
from bcbiovm.common import constant
from bcbiovm.common import utils as common_utils
from bcbiovm.provider import base
from bcbiovm.provider.aws import resources as aws_resources
from bcbiovm.provider.aws import iam as aws_iam
from bcbiovm.provider.aws import icel as aws_icel
from bcbiovm.provider.aws import storage as aws_storage
from bcbiovm.provider.aws import vpc as aws_vpc
from bcbiovm.provider.common import bootstrap as common_bootstrap
from bcbiovm.provider.common import playbook as common_playbook

LOG = logging.get_logger(__name__)


class AWSPlaybook(common_playbook.Playbook):

    """Default paths for Ansible playbooks."""

    icel = ("roles", "icel", "tasks", "main.yml")
    mount_lustre = ("roles", "lustre_client", "tasks", "mount.yml")
    unmount_lustre = ("roles", "lustre_client", "tasks", "unmount.yml")


class AWSProvider(base.BaseCloudProvider):

    """AWS Provider for bcbiovm.

    :ivar flavors: A dictionary with all the flavors available for
                   the current cloud provider.

    Example:
    ::
        flavors = {
            "m3.large": Flavor(cpus=2, memory=3500),
            "m3.xlarge": Flavor(cpus=4, memory=3500),
            "m3.2xlarge": Flavor(cpus=8, memory=3500),
        }
    """

    flavors = {
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
    _STORAGE = {"AmazonS3": aws_storage.AmazonS3}

    def __init__(self):
        super(AWSProvider, self).__init__(name=constant.PROVIDER.AWS)
        self._playbook = AWSPlaybook()
        self._biodata_template = objectstore.BIODATA_INFO["s3"]

    def get_storage_manager(self, name="AmazonS3"):
        """Return a cloud provider specific storage manager.

        :param name: The name of the required storage manager.
        """
        return self._STORAGE.get(name)()

    def information(self, config, cluster):
        """
        Get all the information available for this provider.

        The returned information will be used to create a status report
        regarding the bcbio instances.

        :config:          elasticluster config file
        :cluster:         cluster name
        """
        report = aws_resources.Report(config, cluster)
        report.add_cluster_info()
        report.add_iam_info()
        report.add_security_groups_info()
        report.add_vpc_info()
        report.add_instance_info()
        return report.digest()

    def colect_data(self, config, cluster, rawdir):
        """Collect from the each instances the files which contains
        information regarding resources consumption.

        :param config:    elasticluster config file
        :param cluster:   cluster name
        :param rawdir:    directory where to copy raw collectl data files.

        Notes:
            The current workflow is the following:
                - establish a SSH connection with the instance
                - get information regarding the `collectl` files
                - copy to local the files which contain new information

            This files will be used in order to generate system statistics
            from bcbio runs. The statistics will contain information regarding
            CPU, memory, network, disk I/O usage.
        """
        collector = aws_resources.Collector(config=config, cluster=cluster,
                                            rawdir=rawdir,
                                            playbook=self._playbook)
        return collector.run()

    def resource_usage(self, bcbio_log, rawdir):
        """Generate system statistics from bcbio runs.

        Parse the files obtained by the :meth colect_data: and put the
        information in :class pandas.DataFrame:.

        :param bcbio_log:   local path to bcbio log file written by the run
        :param rawdir:      directory to put raw data files

        :return: a tuple with two dictionaries, the first contains
                 an instance of :pandas.DataFrame: for each host and
                 the second one contains information regarding the
                 hardware configuration
        :type return: tuple
        """
        parser = aws_resources.Parser(bcbio_log, rawdir)
        return parser.run()

    def bootstrap(self, config, cluster, reboot):
        """Install or update the bcbio-nextgen code and the tools
        with the latest version available.

        :param config:    elasticluster config file
        :param cluster:   cluster name
        :param reboot:    whether to upgrade and restart the host OS
        """
        bootstrap = common_bootstrap.Bootstrap(provider=self, config=config,
                                               cluster_name=cluster,
                                               reboot=reboot)
        return bootstrap.run()

    def upload_biodata(self, genome, target, source, context):
        """Upload biodata for a specific genome build and target to a storage
        manager.

        :param genome:  Genome which should be uploaded.
        :param target:  The pice from the genome that should be uploaded.
        :param source:  A list of directories which contain the information
                        that should be uploaded.
        :param context: A dictionary that may contain useful information
                        for the cloud provider (credentials, headers etc).
        """
        storage_manager = self.get_storage_manager()
        biodata = self._biodata_template.format(build=genome, target=target)

        try:
            archive = common_utils.compress(source)
            file_info = storage_manager.parse_remote(biodata)
            if storage_manager.exists(file_info.bucket, file_info.key):
                LOG.info("The %(biodata)r build already exist",
                         {"biodata": file_info.key})
                return
            LOG.info("Upload pre-prepared genome data: %(genome)s, "
                     "%(target)s:", {"genome": genome, "target": target})
            storage_manager.upload(path=archive, filename=file_info.blob,
                                   container=file_info.bucket,
                                   context=context)
        finally:
            if os.path.exists(archive):
                os.remove(archive)

    def bootstrap_iam(self, config, create, recreate):
        """Create IAM users and instance profiles for running bcbio on AWS.

        :param config:      elasticluster bcbio configuration file
        :param create:      whether to create a new IAM user or just generate
                            a configuration file. Useful for users without
                            full permissions to IAM.
        :param recreate:    Recreate current IAM user access keys
        """
        configuration = {}
        iam = aws_iam.IAMOps(config)
        configuration.update(iam.create_keypair(config))
        configuration.update(iam.bcbio_iam_user(create, recreate))
        configuration.update(iam.bcbio_s3_instance_profile(create))
        common_utils.write_elasticluster_config(configuration, config,
                                                self._name)
        return configuration

    def bootstrap_vpc(self, cluster, config, network, recreate):
        """
        Create VPC and associated resources.

        :param config:      elasticluster config file
        :param cluster:     cluster name
        :param network:     network to use for the VPC, in CIDR
                            notation (a.b.c.d/e)
        :param recreate:    whether to recreate the VPC if exists
        """
        vpc = aws_vpc.VirtualPrivateCloud(cluster, config, network, recreate)
        return vpc.run()

    def create_icel(self, cluster, config, **kwargs):
        """Create scratch filesystem using Intel Cloud Edition for Lustre

        :param config:      elasticluster config file
        :param cluster:     cluster name
        :param network:     network (in CIDR notation, a.b.c.d/e)
                            to place Lustre servers in
        :param bucket:      bucket to store generated ICEL template
                            for CloudFormation
        :param stack_name:  CloudFormation name for the new stack
        :param size:        size of the Lustre filesystem, in gigabytes
        :param oss_count:   number of OSS node
        :param lun_count:   number of EBS LUNs per OSS
        :param recreate:    whether to remove and recreate the stack,
                            destroying all data stored on it
        :param setup:       whether to run again the configuration steps
        """
        icel = aws_icel.ICELOps(cluster, config, self._playbook)
        return icel.create(**kwargs)

    def mount_lustre(self, cluster, config, stack_name):
        """Mount Lustre filesystem on all cluster nodes.

        :param config:      elasticluster config file
        :param cluster:     cluster name
        :param stack_name:  CloudFormation name for the new stack
        """
        icel = aws_icel.ICELOps(cluster, config, self._playbook)
        return icel.mount(stack_name)

    def unmount_lustre(self, cluster, config, stack_name):
        """Unmount Lustre filesystem on all cluster nodes.

        :param config:      elasticluster config file
        :param cluster:     cluster name
        :param stack_name:  CloudFormation name for the new stack
        """
        icel = aws_icel.ICELOps(cluster, config, self._playbook)
        return icel.unmount(stack_name)

    def stop_lustre(self, cluster, config, stack_name):
        """Stop the running Lustre filesystem and clean up resources.

        :param config:      elasticluster config file
        :param cluster:     cluster name
        :param stack_name:  CloudFormation name for the new stack
        """
        icel = aws_icel.ICELOps(cluster, config, self._playbook)
        return icel.stop(stack_name)

    def lustre_spec(self, cluster, config, stack_name):
        """Get the filesystem spec for a running filesystem.

        :param config:      elasticluster config file
        :param cluster:     cluster name
        :param stack_name:  CloudFormation name for the new stack
        """
        icel = aws_icel.ICELOps(cluster, config, self._playbook)
        return icel.fs_spec(stack_name)
